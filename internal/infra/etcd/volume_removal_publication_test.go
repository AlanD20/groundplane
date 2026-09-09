package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

// Rationale: the publisher consumes the existing runtime record contract and
// a bounded create-only fragment, not a caller-authored set of store writes.
func TestVolumeRemovalInitialPublicationFragment(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
	if err != nil {
		t.Fatalf("bind initial publication: %v", err)
	}
	defer clearBackupRuntimeMutations(publication.mutations)
	if len(publication.conditions) != 1 || len(publication.mutations) != 3 {
		t.Fatalf(
			"unexpected initial fragment: %d compares, %d mutations",
			len(publication.conditions),
			len(publication.mutations),
		)
	}
	wantKeys := []string{
		removalrecord.RuntimeKey(task.OperationID), removalrecord.AttemptKey(task.OperationID, 1),
		removalrecord.ProgressKey(task.OperationID), removalrecord.PendingPathKey(task.OperationID),
	}
	if publication.conditions[0] != (Condition{Key: removalrecord.Root(task.OperationID), Prefix: true}) {
		t.Fatal("initial fragment does not require complete operation absence")
	}
	runtime, err := removalrecord.DecodeRuntime(publication.mutations[0].Value)
	if err != nil || runtime.CurrentTaskID != task.ID || runtime.Checkpoint != removalrecord.DesiredPublished {
		t.Fatalf("initial runtime encoding: %v", err)
	}
	attempt, err := removalrecord.DecodeAttempt(publication.mutations[1].Value)
	if err != nil || attempt.TaskID != task.ID || attempt.Ordinal != 1 {
		t.Fatalf("initial attempt encoding: %v", err)
	}
	progress, err := removalrecord.DecodeProgress(publication.mutations[2].Value)
	if err != nil || progress.OperationID != task.OperationID || progress.NextRequestOrdinal != 1 {
		t.Fatalf("initial progress encoding: %v", err)
	}
	for index, mutation := range publication.mutations {
		if mutation.Key != wantKeys[index] || mutation.Type != MutationPut || mutation.Prefix {
			t.Fatal("initial record fragment changed key or effect")
		}
	}
	sizer := &store{root: "/groundplane"}
	bytes, err := sizer.transactionSize(publication.conditions, publication.mutations)
	if err != nil || bytes > 8*1024 {
		t.Fatalf("initial fragment exceeds 8 KiB: bytes=%d error=%v", bytes, err)
	}
	t.Logf(
		"initial runtime fragment: %d compares, %d mutations, %d protobuf bytes",
		len(publication.conditions),
		len(publication.mutations),
		bytes,
	)
	empty := newMemoryTaskStore()
	accepted, err := empty.Transact(context.Background(), publication.conditions, publication.mutations)
	if err != nil || !accepted.Succeeded {
		t.Fatalf("empty operation publication control: %v", err)
	}
	for _, occupiedKey := range wantKeys {
		t.Run(occupiedKey, func(t *testing.T) {
			backend := newMemoryTaskStore()
			if _, err := backend.Transact(context.Background(), nil, []Mutation{{Type: MutationPut, Key: occupiedKey, Value: []byte("occupied")}}); err != nil {
				t.Fatal(err)
			}
			before := backend.revision
			result, err := backend.Transact(context.Background(), publication.conditions, publication.mutations)
			if err != nil || result.Succeeded || backend.revision != before {
				t.Fatalf("pre-existing removal state did not exclude publication: %v", err)
			}
		})
	}
}

// Rationale: a valid removal record set cannot authorize a different Task,
// protected intent, original response, or replay target at publication time.
func TestVolumeRemovalInitialPublicationRejectsChangedBinding(t *testing.T) {
	for name, change := range map[string]func(*TaskRecord, *IdempotencyMarker){
		"task id": func(task *TaskRecord, _ *IdempotencyMarker) { task.ID = ids.NewAt(ids.KindTask, task.CreatedAt, 100) },
		"operation": func(task *TaskRecord, _ *IdempotencyMarker) {
			task.OperationID = ids.NewAt(ids.KindOperation, task.CreatedAt, 100)
		},
		"environment": func(task *TaskRecord, _ *IdempotencyMarker) {
			task.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, task.CreatedAt, 100)
		},
		"volume": func(task *TaskRecord, _ *IdempotencyMarker) {
			task.Target = ids.NewAt(ids.KindVolume, task.CreatedAt, 100)
		},
		"timeout":    func(task *TaskRecord, _ *IdempotencyMarker) { task.TimeoutSeconds = 120 },
		"generation": func(task *TaskRecord, _ *IdempotencyMarker) { task.RenderGeneration++ },
		"step": func(task *TaskRecord, _ *IdempotencyMarker) {
			task.Steps[0].ID = ids.NewAt(ids.KindStep, task.CreatedAt, 100)
		},
		"task time": func(task *TaskRecord, _ *IdempotencyMarker) { task.CreatedAt = task.CreatedAt.Add(-time.Second) },
		"task kind": func(task *TaskRecord, _ *IdempotencyMarker) { task.Type = TaskUpdate },
		"wrong intent": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.Intent.Ciphertext = []byte("another-protected-intent")
			digest := sha256.Sum256(marker.Intent.Ciphertext)
			marker.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
		},
		"wrong key":      func(_ *TaskRecord, marker *IdempotencyMarker) { marker.Locator.Key = strings.Repeat("b", 128) },
		"wrong route":    func(_ *TaskRecord, marker *IdempotencyMarker) { marker.Locator.Route = "/tasks/{id}" },
		"missing target": func(_ *TaskRecord, marker *IdempotencyMarker) { marker.ReplayTarget = nil },
		"wrong target": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.ReplayTarget.ID = ids.NewAt(ids.KindVolume, marker.CreatedAt, 100)
		},
		"response status": func(_ *TaskRecord, marker *IdempotencyMarker) { marker.Response.Status = 200 },
		"response shape": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.Response.Body = []byte(`{"task_id":"` + marker.TaskID + `","extra":true}`)
		},
		"marker time": func(_ *TaskRecord, marker *IdempotencyMarker) { marker.UpdatedAt = marker.CreatedAt.Add(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			initial, task, marker := volumeRemovalPublicationFixture(t)
			change(&task, &marker)
			publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
			if err == nil || len(publication.conditions) != 0 || len(publication.mutations) != 0 {
				t.Fatalf("changed binding yielded a usable publication: %v", err)
			}
		})
	}
	_, task, marker := volumeRemovalPublicationFixture(t)
	if _, err := prepareVolumeRemovalInitialPublication(removalrecord.InitialPublication{}, task, marker); err == nil {
		t.Fatal("zero initial preparation accepted")
	}
}

// Rationale: valid Task presentation fields cannot substitute for the immutable
// inputs consumed on assignment. Every removal parameter is exact and closed.
func TestVolumeRemovalInitialPublicationRejectsChangedParameters(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	for key := range task.Params {
		t.Run(key, func(t *testing.T) {
			initial, task, marker := volumeRemovalPublicationFixture(t)
			task.Params[key] = "changed"
			if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
				t.Fatal("changed removal parameter accepted")
			}
			delete(task.Params, key)
			if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
				t.Fatal("missing removal parameter accepted")
			}
		})
	}
	task.Params["extra"] = "unbound"
	if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
		t.Fatal("extra removal parameter accepted")
	}
}

// Rationale: the root response is exactly the original Task acknowledgement,
// even if a caller consistently hashes a different compact JSON shape.
func TestVolumeRemovalInitialPublicationRejectsNonCanonicalRootResponse(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	runtime, _, _, err := initial.Records()
	if err != nil {
		t.Fatal(err)
	}
	marker.Response.Body = []byte(`{"task_id":"` + task.ID + `","extra":true}`)
	runtime.RootResponseSHA256 = sha256.Sum256(marker.Response.Body)
	initial, err = removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepareVolumeRemovalInitialPublication(initial, task, marker); err == nil {
		t.Fatal("non-canonical root response accepted with a matching digest")
	}
}

// Rationale: a reused operation id with only a completion or successor record
// is still occupied. Checking just the three initial records misses this state.
func TestVolumeRemovalInitialPublicationRejectsOrphanedOperationRecords(t *testing.T) {
	initial, task, marker := volumeRemovalPublicationFixture(t)
	publication, err := prepareVolumeRemovalInitialPublication(initial, task, marker)
	if err != nil {
		t.Fatal(err)
	}
	defer clearBackupRuntimeMutations(publication.mutations)
	for _, key := range []string{
		removalrecord.CompletionKey(task.OperationID, 1), removalrecord.AttemptKey(task.OperationID, 2),
	} {
		t.Run(key, func(t *testing.T) {
			backend := newMemoryTaskStore()
			if _, err := backend.Transact(context.Background(), nil, []Mutation{{Type: MutationPut, Key: key, Value: []byte("occupied")}}); err != nil {
				t.Fatal(err)
			}
			before := backend.revision
			result, err := backend.Transact(context.Background(), publication.conditions, publication.mutations)
			if err != nil || result.Succeeded || backend.revision != before {
				t.Fatalf("orphaned operation record did not exclude publication: %v", err)
			}
		})
	}
}

func volumeRemovalPublicationFixture(t *testing.T) (removalrecord.InitialPublication, TaskRecord, IdempotencyMarker) {
	t.Helper()
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.Owner = TaskOwner{WorkspaceType: TaskWorkspaceTenant,
		TenantID: ids.NewAt(ids.KindTenant, now, 20), ProjectID: ids.NewAt(ids.KindProject, now, 21),
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 22)}
	task.Type = TaskRemove
	task.Target = ids.NewAt(ids.KindVolume, now, 23)
	task.TimeoutSeconds = removalrecord.TimeoutSeconds
	task.IdempotencyKey = strings.Repeat("a", 128)
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = task.Owner.EnvironmentID
	marker.Locator.Method = "DELETE"
	marker.Locator.Route = "/volumes/{id}"
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetVolume, ID: task.Target}
	runtime := removalrecord.Runtime{
		OperationID: task.OperationID, EnvironmentID: task.Owner.EnvironmentID, VolumeID: task.Target,
		Key: strings.Repeat("a", 255), DesiredRevisionID: task.ID, DesiredGeneration: uint64(task.RenderGeneration),
		ImpactSHA256: sha256.Sum256([]byte("impact")), EvidenceManifestSHA256: sha256.Sum256([]byte("sealed manifest")),
		IntentSHA256: sha256.Sum256(marker.Intent.Ciphertext), RootResponseSHA256: sha256.Sum256(marker.Response.Body),
		RootLocator: removalrecord.ReplayLocator{
			ScopeKind: string(marker.Locator.ScopeKind),
			ScopeID:   marker.Locator.ScopeID,
			Method:    marker.Locator.Method,
			Route:     marker.Locator.Route,
			Key:       marker.Locator.Key,
		},
		OriginTaskID: task.ID, CurrentTaskID: task.ID, AttemptOrdinal: 1, StepID: task.Steps[0].ID,
		Checkpoint: removalrecord.DesiredPublished, CreatedAt: now, UpdatedAt: now,
	}
	task.Params = map[string]string{
		EnvironmentDesiredRevisionParam: runtime.DesiredRevisionID,
		removalrecord.EnvironmentParam:  runtime.EnvironmentID, removalrecord.OriginTaskParam: task.ID,
		removalrecord.AttemptParam: "1", removalrecord.KeyParam: runtime.Key,
		removalrecord.ImpactParam:   hex.EncodeToString(runtime.ImpactSHA256[:]),
		removalrecord.ManifestParam: hex.EncodeToString(runtime.EvidenceManifestSHA256[:]),
		removalrecord.IntentParam:   hex.EncodeToString(runtime.IntentSHA256[:]),
	}
	initial, err := removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	return initial, task, marker
}
