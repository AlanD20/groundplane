package etcd

import (
	"bytes"
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) acknowledgeHierarchyDeletionAgentTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	claimKey := taskjournal.TaskExecutionClaimKey(taskjournal.TaskExecutorAgent, agentID, taskID)
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
	task, err := decodeTaskRecord(taskValue.Value)
	if err != nil || task.ID != taskID || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion child Task identity changed",
		)
	}
	if err := taskjournal.ValidateTaskResult(result, task.Steps, terminalStatus); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if read.Values[1] == nil {
		if read.Values[2] != nil {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"hierarchy deletion child assignment index is orphaned",
			)
		}
		wantAssignment := taskjournal.TaskTerminalAssignmentRecord{
			AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
		}
		if task.Status != terminalStatus || task.Result == nil || !taskjournal.TaskResultsEqual(*task.Result, result) ||
			task.TerminalAssignment == nil || *task.TerminalAssignment != wantAssignment {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion child Task has no matching assignment",
			)
		}
		if err := repository.ensureHierarchyDeletionReceiptForTask(ctx, task, taskValue.ModRevision); err != nil {
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
			"hierarchy deletion child assignment indexes disagree",
		)
	}
	assignment, err := taskassignments.DecodeTaskAssignment(assignmentValue.Value)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if assignment.TaskID != task.ID || assignment.Executor != taskjournal.TaskExecutorAgent ||
		assignment.AssignmentID != assignmentID || assignment.AgentID != agentID ||
		assignment.AgentGeneration != agentGeneration || task.Status != taskjournal.TaskStatusRunning || task.StartedAt == nil ||
		assignment.ClaimedTaskRevision >= assignmentValue.ModRevision ||
		!assignment.AssignedAt.Equal(*task.StartedAt) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "hierarchy deletion child assignment changed")
	}
	terminalAt, err = nextTaskControllerTimestamp(task.UpdatedAt, terminalAt)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	terminal, err := transitionTaskStatus(task, taskjournal.TaskStatusRunning, terminalStatus, terminalAt)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	terminal.Result = taskjournal.CloneTaskResult(&result)
	terminal.TerminalAssignment = &taskjournal.TaskTerminalAssignmentRecord{
		AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
	}
	if err := validateTaskRecord(terminal); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	terminalValue, err := encodeTaskRecord(terminal)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(terminalValue)
	retentionKey, retentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	defer clear(retentionValue)
	activeKey := taskjournal.TaskActiveOperationKey(task.OperationID)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline)
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{activeKey, taskjournal.TaskQueueKey(task.Executor, task.ID), timeoutKey, retentionKey},
		Revision: read.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if companions == nil || len(companions.Values) != 4 || companions.Values[0] == nil ||
		companions.Values[1] != nil || companions.Values[2] == nil || companions.Values[3] != nil ||
		companions.Values[2].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(companions.Values[2].Value, assignmentValue.Value) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion child lifecycle records disagree",
		)
	}
	activeTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[0].Value)
	if err != nil || activeTaskID != task.ID {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion child active operation is corrupt",
		)
	}
	transaction, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: claimKey, ModRevision: assignmentValue.ModRevision},
			{Key: taskjournal.TaskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
			{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
			{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
			{Key: timeoutKey, ModRevision: companions.Values[2].ModRevision},
			{Key: retentionKey},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: terminalValue},
			{Type: etcdstore.MutationDelete, Key: claimKey},
			{Type: etcdstore.MutationDelete, Key: taskjournal.TaskAssignmentIndexKey(task.ID)},
			{Type: etcdstore.MutationDelete, Key: activeKey},
			{Type: etcdstore.MutationDelete, Key: timeoutKey},
			{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
		},
	)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion child acknowledgement changed",
		)
	}
	if err := repository.ensureHierarchyDeletionReceiptForTask(ctx, terminal, transaction.Revision); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	return etcdstore.Versioned[TaskRecord]{
		Record:       terminal,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, nil
}

func (repository *TaskRepository) ensureHierarchyDeletionReceiptForTask(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
) error {
	parentOperationID := task.Params[taskjournal.TaskHierarchyDeletionParentParam]
	childOperationID := task.Params[taskjournal.TaskHierarchyDeletionChildParam]
	ordinal, err := strconv.ParseInt(task.Params[taskjournal.TaskHierarchyDeletionOrdinalParam], 10, 64)
	if err != nil || ordinal < 0 {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	actionKey, err := hierarchydeletion.HierarchyDeletionActionKey(parentOperationID, ordinal)
	if err != nil {
		return err
	}
	childKey, err := hierarchydeletion.HierarchyDeletionChildKey(parentOperationID, childOperationID)
	if err != nil {
		return err
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{actionKey, childKey}})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	action, err := hierarchydeletion.DecodeHierarchyDeletionAction(read.Values[0].Value)
	if err != nil {
		return err
	}
	entry, err := decodeHierarchyDeletionChildEntry(read.Values[1].Value)
	if err != nil {
		return err
	}
	if action.ParentOperationID != parentOperationID || action.Ordinal != ordinal ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureAgent || action.AgentProcedure == nil ||
		action.AgentProcedure.ChildOperationID != childOperationID ||
		string(action.ActionKind) != task.Params[taskjournal.TaskHierarchyDeletionActionKindParam] ||
		action.AgentProcedure.TypedProcedure != task.Params[taskjournal.TaskHierarchyDeletionProcedureParam] ||
		action.AgentProcedure.InputDigest != task.Params[taskjournal.TaskHierarchyDeletionInputParam] ||
		entry.ParentOperationID != parentOperationID || entry.ChildOperationID != childOperationID ||
		entry.CurrentTaskID != task.ID || entry.CurrentAttemptID != task.Params[taskjournal.TaskHierarchyDeletionAttemptParam] {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	hierarchy, err := newHierarchyDeletionRepository(repository.store)
	if err != nil {
		return err
	}
	operation := HierarchyDeletionOperation{Tombstone: hierarchydeletion.HierarchyDeletionTombstone{OperationID: parentOperationID}}
	return hierarchy.ensureHierarchyDeletionTerminalReceipt(
		ctx, operation, action, entry, read.Values[1].ModRevision, task, taskRevision,
	)
}
