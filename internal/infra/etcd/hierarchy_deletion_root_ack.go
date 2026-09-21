package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

type hierarchyDeletionRootAckChange struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (change *hierarchyDeletionRootAckChange) clear() {
	if change == nil {
		return
	}
	etcdstore.ClearByteSlices(change.values)
	change.values = nil
}

func (repository *TaskRepository) acknowledgeHierarchyDeletionControllerTask(
	ctx context.Context,
	taskID string,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		terminal, err := repository.acknowledgeHierarchyDeletionControllerTaskOnce(
			ctx, taskID, terminalStatus, terminalAt,
		)
		if err == nil {
			return terminal, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		lastErr = err
		if attempt < 7 {
			delay := initialTaskCASDelay << attempt
			if delay > maximumTaskCASDelay {
				delay = maximumTaskCASDelay
			}
			if waitErr := repository.retryPolicy.wait(ctx, repository.retryPolicy.jitter(delay)); waitErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, waitErr
			}
		}
	}
	return etcdstore.Versioned[TaskRecord]{}, lastErr
}

func (repository *TaskRepository) acknowledgeHierarchyDeletionControllerTaskOnce(
	ctx context.Context,
	taskID string,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	claimKey := taskjournal.TaskExecutionClaimKey(taskjournal.TaskExecutorController, "", taskID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(taskID), claimKey, taskjournal.TaskAssignmentIndexKey(taskID),
	}})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if read == nil || len(read.Values) != 3 || read.Values[0] == nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
	}
	taskValue := read.Values[0]
	task, err := DecodeTaskRecord(taskValue.Value)
	if err != nil || task.ID != taskID || task.Executor != taskjournal.TaskExecutorController ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root Task identity changed",
		)
	}
	journal, err := newHierarchyDeletionRepository(repository.store)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if read.Values[1] == nil {
		if read.Values[2] != nil || task.Status != terminalStatus {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion root Task has no matching claim",
			)
		}
		operation, operationErr := journal.OperationByTaskAtRevision(ctx, task.ID, read.ReadRevision)
		if operationErr != nil || operation.Tombstone.Terminal == nil ||
			operation.Tombstone.Terminal.TaskID != task.ID ||
			operation.Tombstone.Terminal.Status != string(terminalStatus) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion root terminal evidence changed",
			)
		}
		if err := repository.validateTaskRetentionReplay(ctx, task, read.ReadRevision); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		return etcdstore.Versioned[TaskRecord]{
			Record:       task,
			Revision:     taskValue.ModRevision,
			ReadRevision: read.ReadRevision,
		}, nil
	}
	assignmentValue := read.Values[1]
	assignmentIndexValue := read.Values[2]
	if assignmentIndexValue == nil || assignmentIndexValue.ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion root assignment indexes disagree",
		)
	}
	assignment, err := taskassignments.DecodeTaskAssignment(assignmentValue.Value)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if assignment.TaskID != task.ID || assignment.Executor != taskjournal.TaskExecutorController ||
		assignment.AgentID != "" || assignment.AgentGeneration != 0 || task.Status != taskjournal.TaskStatusRunning ||
		task.StartedAt == nil || assignment.ClaimedTaskRevision >= assignmentValue.ModRevision ||
		!assignment.AssignedAt.Equal(*task.StartedAt) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root claim changed",
		)
	}
	terminalAt, err = nextTaskControllerTimestamp(task.UpdatedAt, terminalAt)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	terminal, err := TransitionTaskStatus(task, taskjournal.TaskStatusRunning, terminalStatus, terminalAt)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	terminalValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(terminalValue)
	preparedMarker, markerKey, markerRetentionKey, err := prepareTerminalTaskMarker(task, terminalStatus, terminalAt)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(taskRetentionValue)
	activeKey := taskjournal.TaskActiveOperationKey(task.OperationID)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline)
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			activeKey, markerKey, taskjournal.TaskQueueKey(task.Executor, task.ID), markerRetentionKey,
			taskRetentionKey, timeoutKey,
		},
		Revision: read.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if companions == nil || len(companions.Values) != 6 || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] != nil || companions.Values[3] != nil ||
		companions.Values[4] != nil || companions.Values[5] == nil ||
		companions.Values[5].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(companions.Values[5].Value, assignmentValue.Value) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion root lifecycle records disagree",
		)
	}
	if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	preparedMarker, err = hydrateTerminalTaskMarker(preparedMarker, companions.Values[1].Value)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(preparedMarker)
	clear(preparedMarker.Intent.Ciphertext)
	clear(preparedMarker.Response.Body)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(markerValue)
	markerRetentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(markerRetentionValue)
	operation, err := journal.OperationByTaskAtRevision(ctx, task.ID, read.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	change, err := journal.prepareHierarchyDeletionRootAcknowledgement(
		ctx, operation, task, terminalStatus, terminalAt, read.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer change.clear()
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: claimKey, ModRevision: assignmentValue.ModRevision},
		{Key: taskjournal.TaskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
		{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
		{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)}, {Key: markerRetentionKey},
		{Key: taskRetentionKey}, {Key: timeoutKey, ModRevision: companions.Values[5].ModRevision},
	}
	conditions = append(conditions, change.conditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: terminalValue},
		{Type: etcdstore.MutationDelete, Key: claimKey},
		{Type: etcdstore.MutationDelete, Key: taskjournal.TaskAssignmentIndexKey(task.ID)},
		{Type: etcdstore.MutationDelete, Key: activeKey},
		{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue},
		{Type: etcdstore.MutationPut, Key: markerRetentionKey, Value: markerRetentionValue},
		{Type: etcdstore.MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
		{Type: etcdstore.MutationDelete, Key: timeoutKey},
	}
	mutations = append(mutations, change.mutations...)
	if err := hierarchydeletion.EnforceHierarchyDeletionTransaction(conditions, mutations); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root acknowledgement changed",
		)
	}
	return etcdstore.Versioned[TaskRecord]{
		Record:       terminal,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, nil
}
