package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: resource Task hooks must bind the Environment named by both the
// durable resource input and Task owner, never a caller-selected other scope.
func TestOrdinaryTaskEnvironmentMutationRejectsCrossScopeIdentity(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 24, 21, 0, 0, 0, time.UTC)
	task := validTaskRecord(at)
	task.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, at, 1)
	task.Params = map[string]string{testtaskjournal.TaskMutationEnvironmentParam: ids.NewAt(ids.KindEnvironment, at, 2)}
	if _, _, err := ordinaryTaskEnvironmentMutationTarget(task, false, true, false, false, false); !isKind(
		err,
		errs.KindInternal,
	) {
		t.Fatalf("ordinaryTaskEnvironmentMutationTarget(cross scope) error = %v", err)
	}
	task.Params[testtaskjournal.TaskEntryEnvironmentParam] = task.Owner.EnvironmentID
	if _, _, err := ordinaryTaskEnvironmentMutationTarget(task, false, true, true, false, false); !isKind(
		err,
		errs.KindInternal,
	) {
		t.Fatalf("ordinaryTaskEnvironmentMutationTarget(multiple resources) error = %v", err)
	}
}

// Rationale: Task hook reads and compares must stay on one selected revision;
// a later epoch or lock race must perform no domain or epoch writes.
func TestOrdinaryTaskEnvironmentMutationUsesFixedRevisionAndFailsClosed(t *testing.T) {
	t.Parallel()
	_, store, environment, project := backupPolicyRepositoryTestHierarchy(t)
	audited := &environmentMutationFenceAuditStore{memoryHierarchyStore: store}
	repository, err := newTaskRepository(audited)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(time.Date(2026, 8, 24, 21, 10, 0, 0, time.UTC))
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Params = map[string]string{testtaskjournal.TaskServiceEnvironmentParam: environment.Record.ID}
	selectedRevision := store.revision
	sentinel := "/v1/test/task-environment-mutation/no-write"
	binding, err := repository.bindOrdinaryTaskEnvironmentMutation(
		context.Background(),
		task,
		selectedRevision,
		[]testkeyvalue.Condition{{Key: sentinel}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: sentinel, Value: []byte("written")}},
		false,
		false,
		false,
		true,
		false,
	)
	if err != nil {
		t.Fatalf("bindOrdinaryTaskEnvironmentMutation() error = %v", err)
	}
	defer binding.Clear()
	defer testkeyvalue.ClearMutationValues(binding.Mutations())
	for _, revision := range audited.revisions {
		if revision != selectedRevision {
			t.Fatalf("GetMany revision = %d, want %d", revision, selectedRevision)
		}
	}
	advanceEnvironmentMutationFenceEpoch(t, store, environment.Record.ID)
	before := store.revision
	result, err := store.Transact(context.Background(), binding.Conditions(), binding.Mutations())
	if err != nil || result.Succeeded {
		t.Fatalf("Transact(stale epoch) = %#v, %v", result, err)
	}
	conflict := binding.ClassifyConflict(
		result.Revision,
		result.FailureReads,
		func(int64, []*testkeyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "domain changed")
		},
	)
	testkeyvalue.ClearValues(result.FailureReads)
	if !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("classify(stale epoch) error = %v", conflict)
	}
	if store.revision != before || store.valueAt(sentinel, store.revision) != nil {
		t.Fatalf("failed Task Environment transaction wrote state at revision %d", store.revision)
	}
}

// Rationale: the Task hook must reject a canonical operation lock before it
// can publish any resource terminal or retry mutation.
func TestOrdinaryTaskEnvironmentMutationRejectsHeldLockAndOversizedBatch(t *testing.T) {
	t.Parallel()
	_, store, environment, project := backupPolicyRepositoryTestHierarchy(t)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(time.Date(2026, 8, 24, 21, 20, 0, 0, time.UTC))
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Params = map[string]string{TaskConnectorEnvironmentParam: environment.Record.ID}
	putEnvironmentMutationFenceTestLock(
		t,
		store,
		environment.Record.ID,
		environmentMutationFenceTestOwner(task.CreatedAt, 500),
	)
	if _, err := repository.bindOrdinaryTaskEnvironmentMutation(
		context.Background(), task, store.revision, nil, nil, false, false, false, false, true,
	); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("bindOrdinaryTaskEnvironmentMutation(held lock) error = %v", err)
	}

	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationDelete, Key: testhierarchy.EnvironmentOperationLockKey(environment.Record.ID),
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("delete operation lock = %#v, %v", result, err)
	}
	conditions := make([]testkeyvalue.Condition, testkeyvalue.MaximumOperations)
	for index := range conditions {
		conditions[index] = testkeyvalue.Condition{
			Key: "/v1/test/task-environment-mutation/budget/" + ids.NewAt(
				ids.KindTask,
				task.CreatedAt,
				int64(index+600),
			),
		}
	}
	if _, err := repository.bindOrdinaryTaskEnvironmentMutation(
		context.Background(),
		task,
		result.Revision,
		conditions,
		nil,
		false,
		false,
		false,
		false,
		true,
	); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("bindOrdinaryTaskEnvironmentMutation(oversized) error = %v", err)
	}
}
