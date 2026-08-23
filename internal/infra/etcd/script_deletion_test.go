package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestScriptRepositoryRemovalFinalizesOnlyAfterSuccessfulTask(t *testing.T) {
	// Rationale: public Script visibility, the deletion fence, and Task journal
	// must never describe different removal attempts after a Controller crash.
	t.Parallel()
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository(): %v", err)
	}
	record, err := NewScriptRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Name: "migrate", ServiceName: target.Record.Desired.Name,
		Body: "php artisan migrate --force", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	current, err := repository.CreateScript(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateScript(): %v", err)
	}
	task, marker, tombstone := scriptDeletionTestRecords(t, current)
	result, err := repository.BeginScriptDeletionWithTask(
		ctx, environment, project, target, current, tombstone, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginScriptDeletionWithTask(): %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("deletion outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if _, err := repository.GetScript(ctx, current.Record.Desired.ID); err != nil {
		t.Fatalf("GetScript(fenced): %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository(): %v", err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%v/%v", claim, found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(): %v", err)
	}
	for _, key := range []string{
		scriptKey(current.Record.Desired.ID),
		scriptOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
		scriptNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
		deletionTombstoneKey(string(DeletionTargetScript), current.Record.Desired.ID),
	} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
}

func TestScriptRemovalAbortRetainsTargetAndSupportsReplay(t *testing.T) {
	// Rationale: aborting before Controller assignment must release only the
	// fence, retain the Script, and make a duplicate abort provably idempotent.
	t.Parallel()
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository(): %v", err)
	}
	record, err := NewScriptRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Name: "rollback", ServiceName: target.Record.Desired.Name,
		Body: "php artisan migrate:rollback --force", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	current, err := repository.CreateScript(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateScript(): %v", err)
	}
	task, marker, tombstone := scriptDeletionTestRecords(t, current)
	if _, err := repository.BeginScriptDeletionWithTask(
		ctx, environment, project, target, current, tombstone, task, marker,
	); err != nil {
		t.Fatalf("BeginScriptDeletionWithTask(): %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository(): %v", err)
	}
	terminalAt := task.CreatedAt.Add(time.Second)
	if _, err := tasks.AbortPendingTask(ctx, task.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask(): %v", err)
	}
	if _, err := tasks.AbortPendingTask(ctx, task.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask(replay): %v", err)
	}
	if _, err := repository.GetScript(ctx, current.Record.Desired.ID); err != nil {
		t.Fatalf("GetScript(retained): %v", err)
	}
	fence, err := store.Get(ctx, deletionTombstoneKey(string(DeletionTargetScript), current.Record.Desired.ID))
	if err != nil || fence.Entry != nil {
		t.Fatalf("Get(released tombstone) = %#v/%v", fence, err)
	}
}

func scriptDeletionTestRecords(
	t *testing.T,
	script Versioned[ScriptRecord],
) (TaskRecord, IdempotencyMarker, DeletionTombstoneRecord) {
	t.Helper()
	createdAt := serviceRecordTestTime().Add(4 * time.Hour)
	task := validTaskRecord(createdAt)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 1700)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 1701)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 1702)
	task.Executor = TaskExecutorController
	task.Type = TaskRemove
	task.Target = script.Record.Desired.ID
	task.Params = map[string]string{TaskResourceKindParam: TaskResourceScript}
	task.TimeoutSeconds = 30
	task.IdempotencyKey = "script-remove-key-0001"
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: script.Record.EnvironmentID,
		Method: http.MethodDelete, Route: "/scripts/{id}", Key: task.IdempotencyKey,
	}
	replayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetScript, ID: script.Record.Desired.ID}
	marker.ReplayTarget = &replayTarget
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetScript, TargetID: script.Record.Desired.ID,
		TargetRevision: script.Revision, TaskID: task.ID, Phase: DeletionPhaseFinalizing,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return task, marker, tombstone
}
