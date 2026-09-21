package etcd

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestScriptRemovalRejectsActiveExecutionReferences(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository(): %v", err)
	}
	record, err := testscripts.NewRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Slug: "migrate", ServiceName: target.Record.Desired.Name,
		Body: "php artisan migrate --force", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	current, err := repository.CreateScript(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateScript(): %v", err)
	}
	current.Record.ActiveReferences = 1
	task, marker, tombstone := scriptDeletionTestRecords(t, project, environment, current)
	_, err = repository.BeginScriptDeletionWithTask(
		ctx, environment, project, target, current, tombstone, task, marker,
	)
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("BeginScriptDeletionWithTask(active reference) error = %v, want resource.in_use", err)
	}
}

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
	record, err := testscripts.NewRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Slug: "migrate", ServiceName: target.Record.Desired.Name,
		Body: "php artisan migrate --force", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	current, err := repository.CreateScript(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateScript(): %v", err)
	}
	task, marker, tombstone := scriptDeletionTestRecords(t, project, environment, current)
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
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(): %v", err)
	}
	for _, key := range []string{testscripts.ScriptSetScriptKey(current.Record.EnvironmentID, current.Record.ScriptSetGeneration, current.Record.Desired.ID), testscripts.ScriptSetOwnerKey(current.Record.EnvironmentID, current.Record.ScriptSetGeneration, current.Record.Desired.ID), testscripts.ScriptSetSlugKey(current.Record.EnvironmentID, current.Record.ScriptSetGeneration, current.Record.Desired.Slug), testdeletions.TombstoneKey(string(testdeletions.DeletionTargetScript), current.Record.Desired.ID)} {
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
	record, err := testscripts.NewRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Slug: "rollback", ServiceName: target.Record.Desired.Name,
		Body: "php artisan migrate:rollback --force", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	current, err := repository.CreateScript(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateScript(): %v", err)
	}
	task, marker, tombstone := scriptDeletionTestRecords(t, project, environment, current)
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
	fence, err := store.Get(
		ctx,
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetScript), current.Record.Desired.ID),
	)
	if err != nil || fence.Entry != nil {
		t.Fatalf("Get(released tombstone) = %#v/%v", fence, err)
	}
}

func scriptDeletionTestRecords(
	t *testing.T,
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	script testkeyvalue.Versioned[testscripts.Record],
) (TaskRecord, testidempotency.IdempotencyMarker, testdeletions.DeletionTombstoneRecord) {
	t.Helper()
	createdAt := serviceRecordTestTime().Add(4 * time.Hour)
	task := validTaskRecord(createdAt)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 1700)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 1701)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 1702)
	task.Executor = testtaskjournal.TaskExecutorController
	task.Type = testtaskjournal.TaskRemove
	task.Target = script.Record.Desired.ID
	task.Params = map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceScript}
	task.TimeoutSeconds = 30
	task.IdempotencyKey = "script-remove-key-0001"
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: script.Record.EnvironmentID,
		Method: http.MethodDelete, Route: "/scripts/{id}", Key: task.IdempotencyKey,
	}
	replayTarget := testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetScript,
		ID:   script.Record.Desired.ID,
	}
	marker.ReplayTarget = &replayTarget
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetScript, TargetID: script.Record.Desired.ID,
		TargetRevision: script.Revision, TaskID: task.ID, Phase: testdeletions.DeletionPhaseFinalizing,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return task, marker, tombstone
}
