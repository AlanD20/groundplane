package etcd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BACK-13: a lost RESULT acknowledgement must not rerun a completed hook or
// replace its first encrypted facts; changed results and stale execution
// authority must still fail even when a checkpoint already exists.
func TestBackingHookCheckpointReplaysResultWithoutReplacingCiphertext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	store := newMemoryTaskStore()
	task := validTaskRecord(now)
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := NewAttachRepository(&releaseRenderInputTestStore{memoryTaskStore: store})
	if err != nil {
		t.Fatal(err)
	}
	input := BackingHookCheckpointInput{
		TaskID: task.ID, OperationID: task.OperationID,
		AssignmentID: taskEventTestAssignmentID, AgentID: taskEventTestAgentID,
		AgentGeneration: 1, ExecutionEpoch: 1, StepID: task.Steps[0].ID,
		PlanHash: task.PlanHash, AttachID: ids.NewAt(ids.KindAttach, now, 20),
		Event: "attach", State: BackingHookCheckpointStarted, At: now.Add(time.Second),
	}
	started, existing, err := repository.CheckpointBackingHook(ctx, input)
	if err != nil || existing || started.Record.State != BackingHookCheckpointStarted {
		t.Fatalf("start: existing=%v, err=%v", existing, err)
	}
	duplicate, existing, err := repository.CheckpointBackingHook(ctx, input)
	if err != nil || !existing || duplicate.Revision != started.Revision {
		t.Fatalf("repeat start: existing=%v, err=%v", existing, err)
	}
	first, err := NewAttachEncryptedFacts(input.AttachID, 1, "age-x25519", "sha256", []byte("first-envelope"))
	if err != nil {
		t.Fatal(err)
	}
	input.State = BackingHookCheckpointResult
	input.ResultSHA256 = strings.Repeat("b", 64)
	input.Facts = &first
	input.At = now.Add(2 * time.Second)
	store.failAfterCommit(errors.New("acknowledgement lost"))
	if _, _, err := repository.CheckpointBackingHook(ctx, input); err == nil {
		t.Fatal("injected lost acknowledgement was not returned")
	}
	committedRevision := store.currentRevision()
	second, err := NewAttachEncryptedFacts(input.AttachID, 1, "age-x25519", "sha256", []byte("fresh-envelope"))
	if err != nil {
		t.Fatal(err)
	}
	input.Facts = &second
	replayed, existing, err := repository.CheckpointBackingHook(ctx, input)
	if err != nil || !existing {
		t.Fatalf("replay identical result with new encryption: existing=%v, err=%v", existing, err)
	}
	if replayed.Revision != committedRevision || store.currentRevision() != committedRevision ||
		replayed.Record.Facts == nil || !bytes.Equal(replayed.Record.Facts.Ciphertext, first.Ciphertext) {
		t.Fatal("replay replaced the first durable result or wrote another revision")
	}
	changed := input
	changed.ResultSHA256 = strings.Repeat("c", 64)
	if _, _, err := repository.CheckpointBackingHook(ctx, changed); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("changed result must conflict: %v", err)
	}
	stale := input
	stale.ExecutionEpoch++
	if _, _, err := repository.CheckpointBackingHook(ctx, stale); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("stale assignment must conflict: %v", err)
	}
	if store.currentRevision() != committedRevision {
		t.Fatal("rejected checkpoint changed durable state")
	}
}
