package app

import (
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func scriptDeletionTestRecords(
	t *testing.T,
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	script testkeyvalue.Versioned[testscripts.Record],
) (etcd.TaskRecord, testidempotency.IdempotencyMarker, testdeletions.DeletionTombstoneRecord) {
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
