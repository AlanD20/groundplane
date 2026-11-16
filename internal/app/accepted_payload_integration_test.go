package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// A maximum-size authored file bundle crosses the real age envelope boundary
// and bounded MVCC storage batches; this is not a cipher mock or schema test.
func TestAcceptedMaximumBlueprintPayloadRoundTripAndCorruption(t *testing.T) {
	store := newMemoryHierarchyStore()
	store.revision = 1
	repository, err := acceptedactions.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, discard := manualJourneyEncryptedValue(t, "accepted maximum payload")
	clear(discard)
	payloads, err := acceptedaction.NewPayloads(repository, protector)
	if err != nil {
		t.Fatal(err)
	}
	type request struct{ Bundle core.BlueprintBundle }
	original := request{
		Bundle: core.BlueprintBundle{
			RootPath:       "compose.yaml",
			ComposeSources: []string{"compose.yaml"},
			Files: []core.BlueprintFile{
				{
					Path: "compose.yaml",
					Content: append(
						[]byte("services: {}\n#"),
						bytes.Repeat([]byte("a"), core.BlueprintBundleMaxFileBytes-len("services: {}\n#"))...),
				},
				{Path: "one.conf", Content: bytes.Repeat([]byte("b"), core.BlueprintBundleMaxFileBytes)},
				{Path: "two.conf", Content: bytes.Repeat([]byte("c"), core.BlueprintBundleMaxFileBytes)},
			},
			Interpolation: map[string]string{"AUTHORED_INPUT": strings.Repeat("<", 200<<10)},
		},
	}
	if err := original.Bundle.Validate(); err != nil {
		t.Fatal(err)
	}
	taskID := ids.New(ids.KindTask)
	reference, seal, err := acceptedaction.Seal(t.Context(), payloads, taskID, "blueprint-apply", original)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	task := workexecution.Task{
		ID:             taskID,
		Actor:          workexecution.OperatorActor,
		Action:         workexecution.ActionApply,
		TargetID:       ids.New(ids.KindEnvironment),
		AcceptedIntent: &reference,
		CreatedAt:      at,
		UpdatedAt:      at,
		Deadline:       at.Add(time.Hour),
	}
	raw, err := workqueue.EncodeTask(task)
	if err != nil {
		t.Fatal(err)
	}
	result, err := queue.Commit(
		t.Context(),
		seal,
		workqueue.Change{
			Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(taskID), Value: raw}},
		},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("seal maximum payload: %v", err)
	}
	snapshot := workqueue.TaskSnapshot{Task: task, Revision: result.Revision}
	calls := 0
	if _, err := acceptedaction.Open(t.Context(), payloads, snapshot, "blueprint-apply", result.Revision, func(actual request) error {
		calls++
		if !bytes.Equal(actual.Bundle.Files[2].Content, original.Bundle.Files[2].Content) || actual.Bundle.Interpolation["AUTHORED_INPUT"] != original.Bundle.Interpolation["AUTHORED_INPUT"] {
			t.Fatal("maximum bundle bytes changed")
		}
		return nil
	}); err != nil ||
		calls != 1 {
		t.Fatalf("maximum bundle decryption: calls=%d %v", calls, err)
	}
	captured, err := repository.Read(t.Context(), taskID, reference, "blueprint-apply", result.Revision)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(captured.Ciphertext)
	// Recompute storage hashes around a corrupt age part: encryption integrity
	// must still prevent typed plaintext from reaching an operation compiler.
	captured.Ciphertext[int(captured.Record.Parts[0].Bytes)/2] ^= 1
	digest := sha256.Sum256(captured.Ciphertext[:captured.Record.Parts[0].Bytes])
	captured.Record.Parts[0].SHA256 = hex.EncodeToString(digest[:])
	corruptID := ids.New(ids.KindTask)
	corrupt, err := acceptedactions.NewRecord(
		corruptID,
		"blueprint-apply",
		ids.New(ids.KindPlan),
		captured.Record.PlaintextBytes,
		captured.Record.Parts,
		captured.Ciphertext,
	)
	if err != nil {
		t.Fatal(err)
	}
	conditions, mutations, err := repository.Stage(t.Context(), corrupt, captured.Ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	task.ID, task.AcceptedIntent = corruptID, &corrupt.Reference
	raw, err = workqueue.EncodeTask(task)
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(
		mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(corruptID), Value: raw},
	)
	committed, err := store.Transact(t.Context(), conditions, mutations)
	if err != nil || !committed.Succeeded {
		t.Fatal(err)
	}
	if _, err := acceptedaction.Open(t.Context(), payloads, workqueue.TaskSnapshot{Task: task, Revision: committed.Revision}, "blueprint-apply", committed.Revision, func(request) error { calls++; return nil }); err == nil ||
		calls != 1 {
		t.Fatalf("corrupt age part reached compiler: calls=%d error=%v", calls, err)
	}
	// An expired orphan may be removed; an already accepted capture must survive.
	if _, err := repository.CollectDrafts(t.Context(), "", time.Now().UTC().Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Read(t.Context(), taskID, reference, "blueprint-apply", store.revision); err != nil {
		t.Fatalf("accepted ciphertext was collected as an orphan: %v", err)
	}
	orphanID := ids.New(ids.KindTask)
	_, orphanSeal, err := acceptedaction.Seal(t.Context(), payloads, orphanID, "blueprint-apply", original)
	if err != nil {
		t.Fatal(err)
	}
	// Collection winning before final acceptance must invalidate its exact seal;
	// a delayed accept cannot resurrect missing chunks or its compact kind.
	if _, err := repository.CollectDrafts(t.Context(), "", time.Now().UTC().Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	delayed, err := queue.Commit(t.Context(), orphanSeal)
	if err != nil || delayed.Succeeded {
		t.Fatalf("collected orphan accepted with missing ciphertext: %#v %v", delayed, err)
	}
	orphan, err := store.Range(t.Context(), keyvalue.RangeRequest{Prefix: acceptedactions.Prefix(orphanID), Limit: 100})
	if err != nil || len(orphan.Values) != 0 {
		t.Fatalf("orphan ciphertext leaked after collection: %v", err)
	}
	kind, err := store.Get(t.Context(), acceptedactions.KindKey(orphanID))
	if err != nil || kind.Entry != nil {
		t.Fatalf("failed acceptance published compact kind: %v", err)
	}
}
