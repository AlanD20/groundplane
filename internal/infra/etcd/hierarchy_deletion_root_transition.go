package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

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
		etcdstore.GetManyRequest{Keys: []string{replayKey, lockKey}, Revision: revision},
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
	change := hierarchyDeletionRootAckChange{conditions: []etcdstore.Condition{
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
		change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: lockKey})
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
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: replayKey, Value: replayValue},
	)
	return change, nil
}
