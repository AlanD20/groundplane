package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyDeletionRootAckChange struct {
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (change *hierarchyDeletionRootAckChange) clear() {
	if change == nil {
		return
	}
	clearByteSlices(change.values)
	change.values = nil
}

func (repository *TaskRepository) acknowledgeHierarchyDeletionControllerTask(
	ctx context.Context,
	taskID string,
	terminalStatus TaskStatus,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
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
			return Versioned[TaskRecord]{}, err
		}
		lastErr = err
		if attempt < 7 {
			delay := initialTaskCASDelay << attempt
			if delay > maximumTaskCASDelay {
				delay = maximumTaskCASDelay
			}
			if waitErr := repository.retryPolicy.wait(ctx, repository.retryPolicy.jitter(delay)); waitErr != nil {
				return Versioned[TaskRecord]{}, waitErr
			}
		}
	}
	return Versioned[TaskRecord]{}, lastErr
}

func (repository *TaskRepository) acknowledgeHierarchyDeletionControllerTaskOnce(
	ctx context.Context,
	taskID string,
	terminalStatus TaskStatus,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	claimKey := taskExecutionClaimKey(TaskExecutorController, "", taskID)
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
	if err != nil || task.ID != taskID || task.Executor != TaskExecutorController ||
		task.Params[TaskResourceKindParam] != TaskResourceHierarchyDeletion {
		return Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root Task identity changed",
		)
	}
	journal, err := newHierarchyDeletionRepository(repository.store)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if read.Values[1] == nil {
		if read.Values[2] != nil || task.Status != terminalStatus {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion root Task has no matching claim",
			)
		}
		operation, operationErr := journal.OperationByTaskAtRevision(ctx, task.ID, read.ReadRevision)
		if operationErr != nil || operation.Tombstone.Terminal == nil ||
			operation.Tombstone.Terminal.TaskID != task.ID ||
			operation.Tombstone.Terminal.Status != string(terminalStatus) {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion root terminal evidence changed",
			)
		}
		if err := repository.validateTaskRetentionReplay(ctx, task, read.ReadRevision); err != nil {
			return Versioned[TaskRecord]{}, err
		}
		return Versioned[TaskRecord]{
			Record:       task,
			Revision:     taskValue.ModRevision,
			ReadRevision: read.ReadRevision,
		}, nil
	}
	assignmentValue := read.Values[1]
	assignmentIndexValue := read.Values[2]
	if assignmentIndexValue == nil || assignmentIndexValue.ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
		return Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion root assignment indexes disagree",
		)
	}
	assignment, err := decodeTaskAssignment(assignmentValue.Value)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if assignment.TaskID != task.ID || assignment.Executor != TaskExecutorController ||
		assignment.AgentID != "" || assignment.AgentGeneration != 0 || task.Status != TaskStatusRunning ||
		task.StartedAt == nil || assignment.ClaimedTaskRevision >= assignmentValue.ModRevision ||
		!assignment.AssignedAt.Equal(*task.StartedAt) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "hierarchy deletion root claim changed")
	}
	terminalAt, err = nextTaskControllerTimestamp(task.UpdatedAt, terminalAt)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	terminal, err := transitionTaskStatus(task, TaskStatusRunning, terminalStatus, terminalAt)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	terminalValue, err := encodeTaskRecord(terminal)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	defer clear(terminalValue)
	preparedMarker, markerKey, markerRetentionKey, err := prepareTerminalTaskMarker(task, terminalStatus, terminalAt)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	defer clear(taskRetentionValue)
	activeKey := taskActiveOperationKey(task.OperationID)
	timeoutKey := taskTimeoutIndexKey(task.ID, assignment.Deadline)
	companions, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			activeKey, markerKey, taskQueueKey(task.Executor, task.ID), markerRetentionKey,
			taskRetentionKey, timeoutKey,
		},
		Revision: read.ReadRevision,
	})
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if companions == nil || len(companions.Values) != 6 || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] != nil || companions.Values[3] != nil ||
		companions.Values[4] != nil || companions.Values[5] == nil ||
		companions.Values[5].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(companions.Values[5].Value, assignmentValue.Value) {
		return Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion root lifecycle records disagree",
		)
	}
	if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	preparedMarker, err = hydrateTerminalTaskMarker(preparedMarker, companions.Values[1].Value)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	markerValue, err := encodeIdempotencyMarker(preparedMarker)
	clear(preparedMarker.Intent.Ciphertext)
	clear(preparedMarker.Response.Body)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	defer clear(markerValue)
	markerRetentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(markerRetentionValue)
	operation, err := journal.OperationByTaskAtRevision(ctx, task.ID, read.ReadRevision)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	change, err := journal.prepareHierarchyDeletionRootAcknowledgement(
		ctx, operation, task, terminalStatus, terminalAt, read.ReadRevision,
	)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	defer change.clear()
	conditions := []Condition{
		{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: claimKey, ModRevision: assignmentValue.ModRevision},
		{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
		{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
		{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
		{Key: taskQueueKey(task.Executor, task.ID)}, {Key: markerRetentionKey},
		{Key: taskRetentionKey}, {Key: timeoutKey, ModRevision: companions.Values[5].ModRevision},
	}
	conditions = append(conditions, change.conditions...)
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: terminalValue},
		{Type: MutationDelete, Key: claimKey},
		{Type: MutationDelete, Key: taskAssignmentIndexKey(task.ID)},
		{Type: MutationDelete, Key: activeKey},
		{Type: MutationPut, Key: markerKey, Value: markerValue},
		{Type: MutationPut, Key: markerRetentionKey, Value: markerRetentionValue},
		{Type: MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
		{Type: MutationDelete, Key: timeoutKey},
	}
	mutations = append(mutations, change.mutations...)
	if err := enforceHierarchyDeletionTransaction(conditions, mutations); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root acknowledgement changed",
		)
	}
	return Versioned[TaskRecord]{
		Record:       terminal,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRootAcknowledgement(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	task TaskRecord,
	terminalStatus TaskStatus,
	terminalAt time.Time,
	revision int64,
) (hierarchyDeletionRootAckChange, error) {
	if operation.Tombstone.CurrentTaskID != task.ID || operation.Tombstone.Terminal != nil ||
		operation.Fence.CurrentTaskID != task.ID {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	tombstoneKey := HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	replayKey, _ := HierarchyDeletionReplayTargetKey(operation.Tombstone.OperationID)
	lockKey := HierarchyDeletionLockKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)
	auxiliary, err := repository.store.GetMany(
		ctx,
		GetManyRequest{Keys: []string{replayKey, lockKey}, Revision: revision},
	)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	if auxiliary == nil || len(auxiliary.Values) != 2 || auxiliary.Values[0] == nil || auxiliary.Values[1] == nil {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	var replay HierarchyDeletionReplayLocator
	var lock HierarchyDeletionLock
	if decodeHierarchyDeletionRecord(auxiliary.Values[0].Value, hierarchyDeletionSmallRecordBytes, &replay) != nil ||
		decodeHierarchyDeletionRecord(auxiliary.Values[1].Value, hierarchyDeletionSmallRecordBytes, &lock) != nil ||
		replay.ParentOperationID != operation.Tombstone.OperationID || replay.CurrentTaskID != task.ID ||
		lock.ParentOperationID != operation.Tombstone.OperationID || lock.DeletionEpoch != operation.Tombstone.DeletionEpoch {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	retainUntil := terminalAt.Add(TaskRetention)
	nextTombstone := operation.Tombstone
	nextFence := operation.Fence
	replay.RetainUntil = &retainUntil
	nextTombstone.Terminal = &HierarchyDeletionTerminal{
		Status: string(terminalStatus), TaskID: task.ID, CompletedAt: terminalAt,
		RetainUntil: retainUntil, CompletionSummaryDigest: operation.Tombstone.Checkpoint.CompletedPrefixDigest,
	}
	nextFence.Generation++
	nextFence.Dispatch = HierarchyDeletionDispatchClosed
	nextFence.UpdatedAt = terminalAt
	change := hierarchyDeletionRootAckChange{conditions: []Condition{
		{Key: tombstoneKey, ModRevision: operation.TombstoneRevision},
		{Key: fenceKey, ModRevision: operation.FenceRevision},
		{Key: replayKey, ModRevision: auxiliary.Values[0].ModRevision},
		{Key: lockKey, ModRevision: auxiliary.Values[1].ModRevision},
	}}
	if terminalStatus == TaskStatusCompleted {
		completed, err := repository.prepareHierarchyDeletionCompletedRoot(
			ctx, operation, terminalAt, revision, &nextTombstone, &nextFence, &replay,
		)
		if err != nil {
			return hierarchyDeletionRootAckChange{}, err
		}
		change.conditions = append(change.conditions, completed.conditions...)
		change.mutations = append(change.mutations, completed.mutations...)
		change.values = append(change.values, completed.values...)
		change.mutations = append(change.mutations, Mutation{Type: MutationDelete, Key: lockKey})
	}
	tombstoneValue, err := encodeHierarchyDeletionRecord(nextTombstone, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	fenceValue, err := encodeHierarchyDeletionRecord(nextFence, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	replayValue, err := encodeHierarchyDeletionRecord(replay, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		clear(fenceValue)
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	change.values = append(change.values, tombstoneValue, fenceValue, replayValue)
	change.mutations = append(change.mutations,
		Mutation{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		Mutation{Type: MutationPut, Key: fenceKey, Value: fenceValue},
		Mutation{Type: MutationPut, Key: replayKey, Value: replayValue},
	)
	return change, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionCompletedRoot(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	terminalAt time.Time,
	revision int64,
	nextTombstone *HierarchyDeletionTombstone,
	nextFence *HierarchyDeletionCleanupFence,
	replay *HierarchyDeletionReplayLocator,
) (hierarchyDeletionRootAckChange, error) {
	if operation.Tombstone.Phase != HierarchyDeletionFinalizing ||
		operation.Fence.Phase != HierarchyDeletionFinalizing ||
		operation.Fence.Dispatch != HierarchyDeletionDispatchRetiring ||
		operation.Tombstone.PlanCount == nil ||
		operation.Tombstone.PlanDigest == nil ||
		*operation.Tombstone.PlanCount <= 0 ||
		operation.Tombstone.Checkpoint.CompletedCount != *operation.Tombstone.PlanCount-1 ||
		operation.Fence.ActiveActionOrdinal == nil ||
		*operation.Fence.ActiveActionOrdinal != *operation.Tombstone.PlanCount-1 {
		return hierarchyDeletionRootAckChange{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root is not ready",
		)
	}
	rootOrdinal := *operation.Tombstone.PlanCount - 1
	actionKey, _ := HierarchyDeletionActionKey(operation.Tombstone.OperationID, rootOrdinal)
	actionRead, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{actionKey}, Revision: revision})
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	if actionRead == nil || len(actionRead.Values) != 1 || actionRead.Values[0] == nil {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	action, err := decodeHierarchyDeletionAction(actionRead.Values[0].Value)
	if err != nil || action.Ordinal != rootOrdinal || action.ParentOperationID != operation.Tombstone.OperationID ||
		action.TargetID != operation.Tombstone.TargetID || action.ProcedureKind != HierarchyDeletionProcedureController ||
		action.ControllerProcedure == nil || !hierarchyDeletionRootFinalizerMatches(operation.Tombstone.TargetKind, action.ActionKind) {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	if err := repository.validateHierarchyDeletionRootProjection(ctx, operation); err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	effects, err := repository.prepareHierarchyDeletionControllerEffects(ctx, operation, action)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	change := hierarchyDeletionRootAckChange{
		conditions: effects.conditions,
		mutations:  effects.mutations,
		values:     effects.values,
	}
	expected, err := bindHierarchyDeletionControllerProcedure(
		HierarchyDeletionPlannedAction{
			NodeID: action.NodeID, Ordinal: action.Ordinal, ParentOperationID: action.ParentOperationID,
			ActionKind: action.ActionKind, TargetKind: action.TargetKind, TargetID: action.TargetID,
			TargetRevision: action.TargetRevision,
		},
		HierarchyDeletionControllerFinalizerInput{
			Finalizer: action.ControllerProcedure.Finalizer, TargetKind: action.TargetKind,
			TargetID: action.TargetID, FixedInputRevision: action.TargetRevision,
			FixedInputDigest: effects.fixedInputDigest, BatchOrdinal: 0, BatchCount: 1,
		},
	)
	if err != nil || expected != *action.ControllerProcedure {
		change.clear()
		return hierarchyDeletionRootAckChange{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root template changed",
		)
	}
	actionValue, err := encodeHierarchyDeletionAction(action)
	if err != nil {
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	defer clear(actionValue)
	completion := HierarchyDeletionActionCompletion{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		DeletionEpoch: operation.Tombstone.DeletionEpoch, Ordinal: action.Ordinal,
		ActionDigest: hierarchyDeletionBytesDigest(actionValue), TargetKind: action.TargetKind,
		TargetID: action.TargetID, TargetRevision: action.TargetRevision,
		Executor: HierarchyDeletionProcedureController,
		ControllerProof: &HierarchyDeletionControllerCompletionProof{
			Finalizer: action.ControllerProcedure.Finalizer, FixedInputRevision: action.ControllerProcedure.FixedInputRevision,
			CompareTemplateDigest:       action.ControllerProcedure.CompareTemplateDigest,
			MutationTemplateDigest:      action.ControllerProcedure.MutationTemplateDigest,
			PostconditionTemplateDigest: action.ControllerProcedure.PostconditionTemplateDigest,
		},
		CompletedAt: terminalAt,
	}
	completionValue, err := encodeHierarchyDeletionRecord(completion, hierarchyDeletionCompletionRecordBytes)
	if err != nil {
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	nextTombstoneValue, nextFenceValue := advanceHierarchyDeletionCheckpoint(
		operation, action, completion.ActionDigest, action.ControllerProcedure.MutationTemplateDigest, terminalAt,
	)
	*nextTombstone = nextTombstoneValue
	*nextFence = nextFenceValue
	nextTombstone.Phase = HierarchyDeletionRetained
	nextFence.Phase = HierarchyDeletionRetained
	nextFence.Dispatch = HierarchyDeletionDispatchClosed
	nextFence.ActiveActionOrdinal = nil
	nextTombstone.Terminal = &HierarchyDeletionTerminal{
		Status: string(TaskStatusCompleted), TaskID: operation.Tombstone.CurrentTaskID,
		CompletedAt: terminalAt, RetainUntil: terminalAt.Add(TaskRetention),
		CompletionSummaryDigest: nextTombstone.Checkpoint.CompletedPrefixDigest,
	}
	replay.RetainUntil = &nextTombstone.Terminal.RetainUntil
	summary, err := repository.buildHierarchyDeletionSummaries(
		ctx, operation, completionValue, nextTombstone.Checkpoint.CompletedPrefixDigest, terminalAt, revision,
	)
	if err != nil {
		clear(completionValue)
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	change.conditions = append(change.conditions, summary.conditions...)
	change.mutations = append(change.mutations, Mutation{
		Type: MutationPut, Key: mustHierarchyDeletionCompletionKey(operation.Tombstone.OperationID, rootOrdinal), Value: completionValue,
	})
	change.mutations = append(change.mutations, summary.mutations...)
	change.values = append(change.values, completionValue)
	change.values = append(change.values, summary.values...)
	return change, nil
}

func (repository *HierarchyDeletionRepository) validateHierarchyDeletionRootProjection(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) error {
	key := hierarchyDeletionPrimaryKey(operation.Tombstone.TargetKind, operation.Tombstone.TargetID)
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if read.Entry == nil {
		return errs.New(errs.KindStateConflict, "hierarchy deletion root projection is missing")
	}
	defer clear(read.Entry.Value)
	deletionTaskID := ""
	switch operation.Tombstone.TargetKind {
	case HierarchyDeletionTargetTenant:
		record, decodeErr := decodeTenant(read.Entry.Value)
		if decodeErr != nil || record.ID != operation.Tombstone.TargetID {
			return corruptHierarchyDeletion()
		}
		deletionTaskID = record.DeletionTaskID
	case HierarchyDeletionTargetProject, HierarchyDeletionTargetBacking:
		record, decodeErr := decodeProject(read.Entry.Value)
		if decodeErr != nil || record.ID != operation.Tombstone.TargetID {
			return corruptHierarchyDeletion()
		}
		deletionTaskID = record.DeletionTaskID
	case HierarchyDeletionTargetEnvironment:
		record, decodeErr := decodeEnvironment(read.Entry.Value)
		if decodeErr != nil || record.ID != operation.Tombstone.TargetID {
			return corruptHierarchyDeletion()
		}
		deletionTaskID = record.DeletionTaskID
	default:
		return corruptHierarchyDeletion()
	}
	if deletionTaskID != operation.Tombstone.CurrentTaskID {
		return errs.New(errs.KindStateConflict, "hierarchy deletion root Task projection changed")
	}
	return nil
}

func mustHierarchyDeletionCompletionKey(operationID string, ordinal int64) string {
	key, _ := HierarchyDeletionCompletionKey(operationID, ordinal)
	return key
}

func (repository *HierarchyDeletionRepository) buildHierarchyDeletionSummaries(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	rootCompletion []byte,
	finalCheckpoint string,
	completedAt time.Time,
	revision int64,
) (hierarchyDeletionRootAckChange, error) {
	count := *operation.Tombstone.PlanCount
	completionDigest := hierarchyDeletionFoldDigest("gp-deletion-completion-set-v1", operation.Tombstone.OperationID)
	receiptDigest := hierarchyDeletionFoldDigest("gp-deletion-receipt-set-v1", operation.Tombstone.OperationID)
	childCount := int64(0)
	for begin := int64(0); begin < count-1; begin += hierarchyDeletionPlanBatchSize {
		end := min(begin+hierarchyDeletionPlanBatchSize, count-1)
		keys := make([]string, end-begin)
		for ordinal := begin; ordinal < end; ordinal++ {
			keys[ordinal-begin] = mustHierarchyDeletionCompletionKey(operation.Tombstone.OperationID, ordinal)
		}
		read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
		if err != nil {
			return hierarchyDeletionRootAckChange{}, err
		}
		if read == nil || len(read.Values) != len(keys) {
			return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
		}
		for offset, value := range read.Values {
			ordinal := begin + int64(offset)
			if value == nil {
				return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
			}
			var completion HierarchyDeletionActionCompletion
			if decodeHierarchyDeletionRecord(value.Value, hierarchyDeletionCompletionRecordBytes, &completion) != nil ||
				completion.ParentOperationID != operation.Tombstone.OperationID ||
				completion.DeletionEpoch != operation.Tombstone.DeletionEpoch || completion.Ordinal != ordinal {
				return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
			}
			valueDigest := hierarchyDeletionBytesDigest(value.Value)
			completionDigest = hierarchyDeletionFoldDigest(
				"gp-deletion-completion-set-item-v1",
				completionDigest,
				strconv.FormatInt(ordinal, 10),
				valueDigest,
			)
			if completion.AgentProof != nil {
				childCount++
				receiptDigest = hierarchyDeletionFoldDigest(
					"gp-deletion-receipt-set-item-v1",
					receiptDigest,
					strconv.FormatInt(ordinal, 10),
					completion.AgentProof.ReceiptDigest,
				)
			}
		}
	}
	completionDigest = hierarchyDeletionFoldDigest(
		"gp-deletion-completion-set-item-v1",
		completionDigest,
		strconv.FormatInt(count-1, 10),
		hierarchyDeletionBytesDigest(rootCompletion),
	)
	receiptSummary := HierarchyDeletionReceiptSummary{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		DeletionEpoch: operation.Tombstone.DeletionEpoch, ChildCount: childCount,
		OrderedReceiptSetDigest: receiptDigest, FinalCheckpointDigest: finalCheckpoint, CompletedAt: completedAt,
	}
	receiptSummaryValue, err := encodeHierarchyDeletionRecord(receiptSummary, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	completionSummary := HierarchyDeletionCompletionSummary{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		DeletionEpoch: operation.Tombstone.DeletionEpoch, PlanCount: count, PlanDigest: *operation.Tombstone.PlanDigest,
		OrderedCompletionSetDigest: completionDigest,
		AgentReceiptSummaryDigest:  hierarchyDeletionBytesDigest(receiptSummaryValue),
		FinalCheckpointDigest:      finalCheckpoint, CompletedAt: completedAt, RetainUntil: completedAt.Add(TaskRetention),
	}
	completionSummaryValue, err := encodeHierarchyDeletionRecord(completionSummary, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(receiptSummaryValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	receiptCursor := HierarchyDeletionScanCursor{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		TaskID: operation.Tombstone.CurrentTaskID, NextOrdinal: count, Count: childCount,
		OrderedSetDigest: receiptDigest, UpdatedAt: completedAt,
	}
	completionCursor := HierarchyDeletionScanCursor{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		TaskID: operation.Tombstone.CurrentTaskID, NextOrdinal: count, Count: count,
		OrderedSetDigest: completionDigest, UpdatedAt: completedAt,
	}
	receiptCursorValue, err := encodeHierarchyDeletionRecord(receiptCursor, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(receiptSummaryValue)
		clear(completionSummaryValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	completionCursorValue, err := encodeHierarchyDeletionRecord(completionCursor, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(receiptSummaryValue)
		clear(completionSummaryValue)
		clear(receiptCursorValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	receiptSummaryKey, _ := HierarchyDeletionReceiptSummaryKey(operation.Tombstone.OperationID)
	completionSummaryKey, _ := HierarchyDeletionCompletionSummaryKey(operation.Tombstone.OperationID)
	receiptCursorKey, _ := HierarchyDeletionReceiptScanCursorKey(
		operation.Tombstone.OperationID,
		operation.Tombstone.CurrentTaskID,
	)
	completionCursorKey, _ := HierarchyDeletionCompletionScanCursorKey(
		operation.Tombstone.OperationID,
		operation.Tombstone.CurrentTaskID,
	)
	return hierarchyDeletionRootAckChange{
		conditions: []Condition{
			{Key: receiptSummaryKey},
			{Key: completionSummaryKey},
			{Key: receiptCursorKey},
			{Key: completionCursorKey},
			{Key: mustHierarchyDeletionCompletionKey(operation.Tombstone.OperationID, count-1)},
		},
		mutations: []Mutation{
			{Type: MutationPut, Key: receiptSummaryKey, Value: receiptSummaryValue},
			{Type: MutationPut, Key: completionSummaryKey, Value: completionSummaryValue},
			{Type: MutationPut, Key: receiptCursorKey, Value: receiptCursorValue},
			{Type: MutationPut, Key: completionCursorKey, Value: completionCursorValue},
		},
		values: [][]byte{receiptSummaryValue, completionSummaryValue, receiptCursorValue, completionCursorValue},
	}, nil
}
