package etcd

import (
	"bytes"
	"context"
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
	terminalStatus TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	claimKey := taskExecutionClaimKey(TaskExecutorAgent, agentID, taskID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskKey(taskID), claimKey, taskAssignmentIndexKey(taskID),
	}})
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if read == nil || len(read.Values) != 3 || read.Values[0] == nil {
		return Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
	}
	taskValue := read.Values[0]
	task, err := decodeTaskRecord(taskValue.Value)
	if err != nil || task.ID != taskID || task.Executor != TaskExecutorAgent ||
		task.Params[TaskResourceKindParam] != TaskResourceHierarchyDeletion {
		return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "hierarchy deletion child Task identity changed")
	}
	if err := validateTaskResult(result, task.Steps, terminalStatus); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if read.Values[1] == nil {
		if read.Values[2] != nil {
			return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "hierarchy deletion child assignment index is orphaned")
		}
		wantAssignment := TaskTerminalAssignmentRecord{
			AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
		}
		if task.Status != terminalStatus || task.Result == nil || !taskResultsEqual(*task.Result, result) ||
			task.TerminalAssignment == nil || *task.TerminalAssignment != wantAssignment {
			return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "hierarchy deletion child Task has no matching assignment")
		}
		if err := repository.ensureHierarchyDeletionReceiptForTask(ctx, task, taskValue.ModRevision); err != nil {
			return Versioned[TaskRecord]{}, err
		}
		return Versioned[TaskRecord]{Record: task, Revision: taskValue.ModRevision, ReadRevision: read.ReadRevision}, nil
	}
	assignmentValue := read.Values[1]
	assignmentIndexValue := read.Values[2]
	if assignmentIndexValue == nil || assignmentIndexValue.ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "hierarchy deletion child assignment indexes disagree")
	}
	assignment, err := decodeTaskAssignment(assignmentValue.Value)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if assignment.TaskID != task.ID || assignment.Executor != TaskExecutorAgent ||
		assignment.AssignmentID != assignmentID || assignment.AgentID != agentID ||
		assignment.AgentGeneration != agentGeneration || task.Status != TaskStatusRunning || task.StartedAt == nil ||
		assignment.ClaimedTaskRevision >= assignmentValue.ModRevision ||
		!assignment.AssignedAt.Equal(*task.StartedAt) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "hierarchy deletion child assignment changed")
	}
	terminalAt, err = nextTaskControllerTimestamp(task.UpdatedAt, terminalAt)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	terminal, err := transitionTaskStatus(task, TaskStatusRunning, terminalStatus, terminalAt)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	terminal.Result = cloneTaskResult(&result)
	terminal.TerminalAssignment = &TaskTerminalAssignmentRecord{
		AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
	}
	if err := validateTaskRecord(terminal); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	terminalValue, err := encodeTaskRecord(terminal)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	defer clear(terminalValue)
	retentionKey, retentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	defer clear(retentionValue)
	activeKey := taskActiveOperationKey(task.OperationID)
	timeoutKey := taskTimeoutIndexKey(task.ID, assignment.Deadline)
	companions, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{activeKey, taskQueueKey(task.Executor, task.ID), timeoutKey, retentionKey},
		Revision: read.ReadRevision,
	})
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if companions == nil || len(companions.Values) != 4 || companions.Values[0] == nil ||
		companions.Values[1] != nil || companions.Values[2] == nil || companions.Values[3] != nil ||
		companions.Values[2].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(companions.Values[2].Value, assignmentValue.Value) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "hierarchy deletion child lifecycle records disagree")
	}
	activeTaskID, err := decodeTaskReference(companions.Values[0].Value)
	if err != nil || activeTaskID != task.ID {
		return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "hierarchy deletion child active operation is corrupt")
	}
	transaction, err := repository.store.Transact(ctx,
		[]Condition{
			{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: claimKey, ModRevision: assignmentValue.ModRevision},
			{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
			{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
			{Key: taskQueueKey(task.Executor, task.ID)},
			{Key: timeoutKey, ModRevision: companions.Values[2].ModRevision},
			{Key: retentionKey},
		},
		[]Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: terminalValue},
			{Type: MutationDelete, Key: claimKey},
			{Type: MutationDelete, Key: taskAssignmentIndexKey(task.ID)},
			{Type: MutationDelete, Key: activeKey},
			{Type: MutationDelete, Key: timeoutKey},
			{Type: MutationPut, Key: retentionKey, Value: retentionValue},
		},
	)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "hierarchy deletion child acknowledgement changed")
	}
	if err := repository.ensureHierarchyDeletionReceiptForTask(ctx, terminal, transaction.Revision); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	return Versioned[TaskRecord]{Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision}, nil
}

func (repository *TaskRepository) ensureHierarchyDeletionReceiptForTask(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
) error {
	parentOperationID := task.Params[TaskHierarchyDeletionParentParam]
	childOperationID := task.Params[TaskHierarchyDeletionChildParam]
	ordinal, err := strconv.ParseInt(task.Params[TaskHierarchyDeletionOrdinalParam], 10, 64)
	if err != nil || ordinal < 0 {
		return corruptHierarchyDeletion()
	}
	actionKey, err := HierarchyDeletionActionKey(parentOperationID, ordinal)
	if err != nil {
		return err
	}
	childKey, err := HierarchyDeletionChildKey(parentOperationID, childOperationID)
	if err != nil {
		return err
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{actionKey, childKey}})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return corruptHierarchyDeletion()
	}
	action, err := decodeHierarchyDeletionAction(read.Values[0].Value)
	if err != nil {
		return err
	}
	entry, err := decodeHierarchyDeletionChildEntry(read.Values[1].Value)
	if err != nil {
		return err
	}
	if action.ParentOperationID != parentOperationID || action.Ordinal != ordinal ||
		action.ProcedureKind != HierarchyDeletionProcedureAgent || action.AgentProcedure == nil ||
		action.AgentProcedure.ChildOperationID != childOperationID ||
		string(action.ActionKind) != task.Params[TaskHierarchyDeletionActionKindParam] ||
		action.AgentProcedure.TypedProcedure != task.Params[TaskHierarchyDeletionProcedureParam] ||
		action.AgentProcedure.InputDigest != task.Params[TaskHierarchyDeletionInputParam] ||
		entry.ParentOperationID != parentOperationID || entry.ChildOperationID != childOperationID ||
		entry.CurrentTaskID != task.ID || entry.CurrentAttemptID != task.Params[TaskHierarchyDeletionAttemptParam] {
		return corruptHierarchyDeletion()
	}
	hierarchy, err := newHierarchyDeletionRepository(repository.store)
	if err != nil {
		return err
	}
	operation := HierarchyDeletionOperation{Tombstone: HierarchyDeletionTombstone{OperationID: parentOperationID}}
	return hierarchy.ensureHierarchyDeletionTerminalReceipt(
		ctx, operation, action, entry, read.Values[1].ModRevision, task, taskRevision,
	)
}
