package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) retryHierarchyDeletionTask(
	ctx context.Context,
	source Versioned[TaskRecord],
	retryTaskID string,
	actor TaskActor,
	provided *TaskInitiation,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	retry, err := cloneRetryTask(source.Record, retryTaskID, actor, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if retry.Executor != TaskExecutorController {
		return IdempotencyTransactionResult{}, errs.New(errs.KindTaskNotRetryable, "hierarchy child Tasks retry through their parent operation")
	}
	initiation, err := newInheritedTaskInitiation(source, actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if provided != nil {
		if err := validateTaskInitiation(TaskRecord{}, *provided, false); err != nil || provided.actor != TaskActorSystem {
			return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "system hierarchy Task retry initiation is invalid")
		}
		fences := append(append([]Condition(nil), initiation.fences...), provided.fences...)
		initiation, err = newTaskInitiation(source.Record.Owner, TaskActorSystem, fences...)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != retry.ID || !marker.CreatedAt.Equal(retry.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || validateIdempotencyMarker(marker) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "hierarchy Task retry marker is invalid")
	}
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(retry); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	taskValue, err := encodeTaskRecord(retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(retry.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	change, err := repository.prepareHierarchyDeletionRetry(ctx, source, retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer change.clear()
	conditions := []Condition{
		{Key: taskKey(source.Record.ID), ModRevision: source.Revision}, {Key: taskKey(retry.ID)},
		{Key: taskOperationIndexKey(retry.OperationID, retry.ID)}, {Key: taskActiveOperationKey(retry.OperationID)},
		{Key: taskQueueKey(retry.Executor, retry.ID)},
	}
	conditions = append(conditions, change.conditions...)
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(retry.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(retry.OperationID, retry.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(retry.Executor, retry.ID), Value: reference},
	}
	mutations = append(mutations, change.mutations...)
	baseConditions := len(conditions)
	classifier := func(_ int64, values []*KeyValue) error {
		if len(values) != baseConditions {
			return errs.New(errs.KindInternal, "hierarchy Task retry compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.Newf(errs.KindTaskNotFound, "task not found: %s", source.Record.ID)
		}
		if values[3] != nil {
			activeTaskID, decodeErr := decodeTaskReference(values[3].Value)
			if decodeErr != nil {
				return decodeErr
			}
			return errs.Newf(errs.KindTaskRetryInFlight, "operation %s already has active retry %s", retry.OperationID, activeTaskID)
		}
		return errs.New(errs.KindStateConflict, "hierarchy deletion retry authority changed")
	}
	plan, err := newTaskIdempotencyMutationPlan(retry, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *TaskRepository) prepareHierarchyDeletionRetry(
	ctx context.Context,
	source Versioned[TaskRecord],
	retry TaskRecord,
) (hierarchyDeletionRootAckChange, error) {
	journal, err := newHierarchyDeletionRepository(repository.store)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	operation, err := journal.OperationByTaskAtRevision(ctx, source.Record.ID, source.ReadRevision)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	if operation.Tombstone.CurrentTaskID != source.Record.ID || operation.Tombstone.Terminal == nil ||
		operation.Tombstone.Terminal.TaskID != source.Record.ID ||
		operation.Tombstone.Terminal.Status != string(source.Record.Status) ||
		operation.Tombstone.Phase == HierarchyDeletionRetained || operation.Fence.Dispatch != HierarchyDeletionDispatchClosed {
		return hierarchyDeletionRootAckChange{}, errs.New(errs.KindTaskNotRetryable, "hierarchy deletion operation is not retryable")
	}
	targetKey := hierarchyDeletionPrimaryKey(operation.Tombstone.TargetKind, operation.Tombstone.TargetID)
	tombstoneKey := HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	replayKey, _ := HierarchyDeletionReplayTargetKey(operation.Tombstone.OperationID)
	lockKey := HierarchyDeletionLockKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{targetKey, replayKey, lockKey}, Revision: source.ReadRevision,
	})
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	if read == nil || len(read.Values) != 3 || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	targetValue, err := hierarchyDeletionRetryTargetValue(
		operation.Tombstone.TargetKind, read.Values[0].Value, source.Record.ID, retry.ID,
	)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	var replay HierarchyDeletionReplayLocator
	var lock HierarchyDeletionLock
	if decodeHierarchyDeletionRecord(read.Values[1].Value, hierarchyDeletionSmallRecordBytes, &replay) != nil ||
		decodeHierarchyDeletionRecord(read.Values[2].Value, hierarchyDeletionSmallRecordBytes, &lock) != nil ||
		replay.ParentOperationID != operation.Tombstone.OperationID || replay.CurrentTaskID != source.Record.ID ||
		lock.ParentOperationID != operation.Tombstone.OperationID {
		clear(targetValue)
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	nextTombstone := operation.Tombstone
	nextTombstone.CurrentTaskID = retry.ID
	nextTombstone.AttemptDeadline = retry.CreatedAt.Add(hierarchyDeletionAttemptTimeout)
	nextTombstone.Terminal = nil
	nextFence := operation.Fence
	nextFence.CurrentTaskID = retry.ID
	nextFence.Generation++
	nextFence.UpdatedAt = retry.CreatedAt
	if nextFence.Phase == HierarchyDeletionFinalizing {
		nextFence.Dispatch = HierarchyDeletionDispatchRetiring
	} else {
		nextFence.Dispatch = HierarchyDeletionDispatchOpen
	}
	replay.CurrentTaskID = retry.ID
	replay.RetainUntil = nil
	tombstoneValue, err := encodeHierarchyDeletionRecord(nextTombstone, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		clear(targetValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	fenceValue, err := encodeHierarchyDeletionRecord(nextFence, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(targetValue)
		clear(tombstoneValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	replayValue, err := encodeHierarchyDeletionRecord(replay, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(targetValue)
		clear(tombstoneValue)
		clear(fenceValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	return hierarchyDeletionRootAckChange{
		conditions: []Condition{
			{Key: targetKey, ModRevision: read.Values[0].ModRevision},
			{Key: tombstoneKey, ModRevision: operation.TombstoneRevision},
			{Key: fenceKey, ModRevision: operation.FenceRevision},
			{Key: replayKey, ModRevision: read.Values[1].ModRevision},
			{Key: lockKey, ModRevision: read.Values[2].ModRevision},
		},
		mutations: []Mutation{
			{Type: MutationPut, Key: targetKey, Value: targetValue},
			{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
			{Type: MutationPut, Key: fenceKey, Value: fenceValue},
			{Type: MutationPut, Key: replayKey, Value: replayValue},
		},
		values: [][]byte{targetValue, tombstoneValue, fenceValue, replayValue},
	}, nil
}

func hierarchyDeletionRetryTargetValue(
	kind HierarchyDeletionTargetKind,
	value []byte,
	sourceTaskID string,
	retryTaskID string,
) ([]byte, error) {
	switch kind {
	case HierarchyDeletionTargetTenant:
		record, err := decodeTenant(value)
		if err != nil || record.DeletionTaskID != sourceTaskID {
			return nil, corruptHierarchyDeletion()
		}
		record.DeletionTaskID = retryTaskID
		return encodeTenant(record)
	case HierarchyDeletionTargetProject, HierarchyDeletionTargetBacking:
		record, err := decodeProject(value)
		if err != nil || record.DeletionTaskID != sourceTaskID {
			return nil, corruptHierarchyDeletion()
		}
		record.DeletionTaskID = retryTaskID
		return encodeProject(record)
	case HierarchyDeletionTargetEnvironment:
		record, err := decodeEnvironment(value)
		if err != nil || record.DeletionTaskID != sourceTaskID {
			return nil, corruptHierarchyDeletion()
		}
		record.DeletionTaskID = retryTaskID
		return encodeEnvironment(record)
	default:
		return nil, errs.New(errs.KindValidationFailed, "hierarchy deletion retry target is invalid")
	}
}
