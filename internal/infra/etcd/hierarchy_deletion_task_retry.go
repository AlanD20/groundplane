package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) retryHierarchyDeletionTask(
	ctx context.Context,
	source etcdstore.Versioned[TaskRecord],
	retryTaskID string,
	actor taskjournal.TaskActor,
	provided *TaskInitiation,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	retry, err := CloneRetryTask(source.Record, retryTaskID, actor, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if retry.Executor != taskjournal.TaskExecutorController {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable,
			"hierarchy child Tasks retry through their parent operation",
		)
	}
	initiation, err := newInheritedTaskInitiation(source, actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if provided != nil {
		if err := validateTaskInitiation(TaskRecord{}, *provided, false); err != nil ||
			provided.actor != taskjournal.TaskActorSystem {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"system hierarchy Task retry initiation is invalid",
			)
		}
		fences := append(append([]etcdstore.Condition(nil), initiation.fences...), provided.fences...)
		initiation, err = newTaskInitiation(source.Record.Owner, taskjournal.TaskActorSystem, fences...)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != retry.ID || !marker.CreatedAt.Equal(retry.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) ||
		idempotencyrecord.ValidateIdempotencyMarker(marker) != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy Task retry marker is invalid",
		)
	}
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(retry); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	taskValue, err := EncodeTaskRecord(retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(retry.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	change, err := repository.prepareHierarchyDeletionRetry(ctx, source, retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer change.clear()
	conditions := []etcdstore.Condition{
		{
			Key:         taskjournal.TaskStorageKey(source.Record.ID),
			ModRevision: source.Revision,
		}, {Key: taskjournal.TaskStorageKey(retry.ID)},
		{
			Key: taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID),
		}, {Key: taskjournal.TaskActiveOperationKey(retry.OperationID)},
		{Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID)},
	}
	conditions = append(conditions, change.conditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(retry.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID), Value: reference},
	}
	mutations = append(mutations, change.mutations...)
	baseConditions := len(conditions)
	classifier := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseConditions {
			return errs.New(errs.KindInternal, "hierarchy Task retry compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.Newf(errs.KindTaskNotFound, "task not found: %s", source.Record.ID)
		}
		if values[3] != nil {
			activeTaskID, decodeErr := idempotencyrecord.DecodeTaskReference(values[3].Value)
			if decodeErr != nil {
				return decodeErr
			}
			return errs.Newf(
				errs.KindTaskRetryInFlight,
				"operation %s already has active retry %s",
				retry.OperationID,
				activeTaskID,
			)
		}
		return errs.New(errs.KindStateConflict, "hierarchy deletion retry authority changed")
	}
	plan, err := newTaskIdempotencyMutationPlan(retry, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *TaskRepository) prepareHierarchyDeletionRetry(
	ctx context.Context,
	source etcdstore.Versioned[TaskRecord],
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
		operation.Tombstone.Phase == hierarchydeletion.HierarchyDeletionRetained || operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchClosed {
		return hierarchyDeletionRootAckChange{}, errs.New(
			errs.KindTaskNotRetryable,
			"hierarchy deletion operation is not retryable",
		)
	}
	targetKey := hierarchyDeletionPrimaryKey(operation.Tombstone.TargetKind, operation.Tombstone.TargetID)
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(
		string(operation.Tombstone.TargetKind),
		operation.Tombstone.TargetID,
	)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	replayKey, _ := hierarchydeletion.HierarchyDeletionReplayTargetKey(operation.Tombstone.OperationID)
	lockKey := hierarchydeletion.HierarchyDeletionLockKey(
		string(operation.Tombstone.TargetKind),
		operation.Tombstone.TargetID,
	)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{targetKey, replayKey, lockKey}, Revision: source.ReadRevision,
	})
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	if read == nil || len(read.Values) != 3 || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	targetValue, err := hierarchyDeletionRetryTargetValue(
		operation.Tombstone.TargetKind, read.Values[0].Value, source.Record.ID, retry.ID,
	)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	var replay hierarchydeletion.HierarchyDeletionReplayLocator
	var lock hierarchydeletion.HierarchyDeletionLock
	if hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[1].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &replay) != nil ||
		hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[2].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &lock) != nil ||
		replay.ParentOperationID != operation.Tombstone.OperationID ||
		replay.CurrentTaskID != source.Record.ID ||
		lock.ParentOperationID != operation.Tombstone.OperationID {
		clear(targetValue)
		return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	nextTombstone := operation.Tombstone
	nextTombstone.CurrentTaskID = retry.ID
	nextTombstone.AttemptDeadline = retry.CreatedAt.Add(hierarchydeletion.AttemptTimeout)
	nextTombstone.Terminal = nil
	nextFence := operation.Fence
	nextFence.CurrentTaskID = retry.ID
	nextFence.Generation++
	nextFence.UpdatedAt = retry.CreatedAt
	if nextFence.Phase == hierarchydeletion.HierarchyDeletionFinalizing {
		nextFence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchRetiring
	} else {
		nextFence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchOpen
	}
	replay.CurrentTaskID = retry.ID
	replay.RetainUntil = nil
	tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextTombstone,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
	)
	if err != nil {
		clear(targetValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextFence,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		clear(targetValue)
		clear(tombstoneValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	replayValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		replay,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		clear(targetValue)
		clear(tombstoneValue)
		clear(fenceValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	return hierarchyDeletionRootAckChange{
		conditions: []etcdstore.Condition{
			{Key: targetKey, ModRevision: read.Values[0].ModRevision},
			{Key: tombstoneKey, ModRevision: operation.TombstoneRevision},
			{Key: fenceKey, ModRevision: operation.FenceRevision},
			{Key: replayKey, ModRevision: read.Values[1].ModRevision},
			{Key: lockKey, ModRevision: read.Values[2].ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: targetKey, Value: targetValue},
			{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
			{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue},
			{Type: etcdstore.MutationPut, Key: replayKey, Value: replayValue},
		},
		values: [][]byte{targetValue, tombstoneValue, fenceValue, replayValue},
	}, nil
}

func hierarchyDeletionRetryTargetValue(
	kind hierarchydeletion.HierarchyDeletionTargetKind,
	value []byte,
	sourceTaskID string,
	retryTaskID string,
) ([]byte, error) {
	switch kind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		record, err := hierarchyrecord.DecodeTenant(value)
		if err != nil || record.DeletionTaskID != sourceTaskID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record.DeletionTaskID = retryTaskID
		return hierarchyrecord.EncodeTenant(record)
	case hierarchydeletion.HierarchyDeletionTargetProject, hierarchydeletion.HierarchyDeletionTargetBacking:
		record, err := hierarchyrecord.DecodeProject(value)
		if err != nil || record.DeletionTaskID != sourceTaskID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record.DeletionTaskID = retryTaskID
		return hierarchyrecord.EncodeProject(record)
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		record, err := hierarchyrecord.DecodeEnvironment(value)
		if err != nil || record.DeletionTaskID != sourceTaskID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record.DeletionTaskID = retryTaskID
		return hierarchyrecord.EncodeEnvironment(record)
	default:
		return nil, errs.New(errs.KindValidationFailed, "hierarchy deletion retry target is invalid")
	}
}
