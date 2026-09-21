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
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func secretDeletionTestTask(
	t *testing.T,
	current testkeyvalue.Versioned[testsecrets.Record],
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	createdAt time.Time,
	entropy int64,
) (etcd.TaskRecord, testidempotency.IdempotencyMarker, testdeletions.DeletionTombstoneRecord) {
	task := validTaskRecord(createdAt)
	task.Owner = mustProjectTaskOwner(t, project.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, entropy)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, entropy+1)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, entropy+2)
	task.Executor = testtaskjournal.TaskExecutorController
	task.Type = testtaskjournal.TaskRemove
	task.Target = current.Record.Secret.ID
	task.Params = map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceSecret}
	task.TimeoutSeconds = 30
	task.IdempotencyKey = "secret-remove-key-0001"
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeProject, ScopeID: project.Record.ID,
		Method: http.MethodDelete, Route: "/secrets/{id}", Key: task.IdempotencyKey,
	}
	target := testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetSecret,
		ID:   task.Target,
	}
	marker.ReplayTarget = &target
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetSecret, TargetID: task.Target,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: testdeletions.DeletionPhaseFinalizing,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return task, marker, tombstone
}
