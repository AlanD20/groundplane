package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: failure releases Entry retirement, allowing a Script reservation.
// A retry must not steal that generation, and must work once it is released.
func TestEntryDeletionRetryCannotReacquireAfterScriptPreparation(t *testing.T) {
	ctx := context.Background()
	entries, store, environment, project, current, generationIDs := entryDeletionTestState(t)
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
	result, err := entries.BeginEntryDeletionWithTask(
		ctx, environment, project, current, nil, tombstone, intent, task, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("initial removal = %v / %v", err, result.conflict)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("initial claim = %t, %v", found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusFailed, task.CreatedAt.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	member := entryScriptTestMember(t, store, current.Record, generationIDs[0])
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	members := []ScriptSourcePreparationMember{member}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	retryAt := task.CreatedAt.Add(3 * time.Second)
	retryID := ids.New(ids.KindTask)
	retryMarker := pendingRetryMarker(failed.Record, retryID, retryAt, "entry-script-protected-retry")
	before := store.revision
	if _, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker); !isKind(
		err,
		errs.KindResourceInUse,
	) ||
		store.revision != before {
		t.Fatalf("Entry retry stole prepared Script generation: %v", err)
	}
	if err := authority.Abandon(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	result, err = tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("Entry retry remained blocked after release: %v / %v", err, result.conflict)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, retryAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("retry claim = %t, %v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, retryID, TaskStatusCompleted, retryAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := entries.GetEntry(ctx, current.Record.Entry.ID); !isKind(err, errs.KindEntryNotFound) {
		t.Fatalf("unblocked Entry removal did not finish: %v", err)
	}
	assertEntryGenerations(t, store, current.Record.Entry.ID, generationIDs, false)
}
