package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRootAcknowledgement(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (hierarchyDeletionRootAckChange, error) {
	if operation.Tombstone.CurrentTaskID != task.ID || operation.Tombstone.Terminal != nil ||
		operation.Fence.CurrentTaskID != task.ID {
		return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	replayKey, _ := hierarchydeletion.HierarchyDeletionReplayTargetKey(operation.Tombstone.OperationID)
	lockKey := hierarchydeletion.HierarchyDeletionLockKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)
	auxiliary, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{replayKey, lockKey}, Revision: revision},
	)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	if auxiliary == nil || len(auxiliary.Values) != 2 || auxiliary.Values[0] == nil || auxiliary.Values[1] == nil {
		return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var replay hierarchydeletion.HierarchyDeletionReplayLocator
	var lock hierarchydeletion.HierarchyDeletionLock
	if hierarchydeletion.DecodeHierarchyDeletionRecord(auxiliary.Values[0].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &replay) != nil ||
		hierarchydeletion.DecodeHierarchyDeletionRecord(auxiliary.Values[1].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &lock) != nil ||
		replay.ParentOperationID != operation.Tombstone.OperationID || replay.CurrentTaskID != task.ID ||
		lock.ParentOperationID != operation.Tombstone.OperationID || lock.DeletionEpoch != operation.Tombstone.DeletionEpoch {
		return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	retainUntil := terminalAt.Add(TaskRetention)
	nextTombstone := operation.Tombstone
	nextFence := operation.Fence
	replay.RetainUntil = &retainUntil
	nextTombstone.Terminal = &hierarchydeletion.HierarchyDeletionTerminal{
		Status: string(terminalStatus), TaskID: task.ID, CompletedAt: terminalAt,
		RetainUntil: retainUntil, CompletionSummaryDigest: operation.Tombstone.Checkpoint.CompletedPrefixDigest,
	}
	nextFence.Generation++
	nextFence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchClosed
	nextFence.UpdatedAt = terminalAt
	change := hierarchyDeletionRootAckChange{conditions: []etcdstore.Condition{
		{Key: tombstoneKey, ModRevision: operation.TombstoneRevision},
		{Key: fenceKey, ModRevision: operation.FenceRevision},
		{Key: replayKey, ModRevision: auxiliary.Values[0].ModRevision},
		{Key: lockKey, ModRevision: auxiliary.Values[1].ModRevision},
	}}
	if terminalStatus == taskjournal.TaskStatusCompleted {
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
	tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(nextTombstone, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
	if err != nil {
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(nextFence, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	replayValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(replay, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
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
