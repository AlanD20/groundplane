package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareHierarchyDeletionTaskPrune(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention etcdstore.KeyValue,
	revision int64,
	now time.Time,
) (bool, bool, error) {
	markerKey, err := idempotencyMarkerKey(*task.idempotencyMarker)
	if err != nil {
		return false, false, corruptHierarchyDeletion()
	}
	operationID := task.Params[TaskHierarchyDeletionOperationParam]
	if !validHierarchyDeletionPrivateID(operationID, "del") {
		return false, false, corruptHierarchyDeletion()
	}
	intentKey, _ := HierarchyDeletionIntentKey(operationID)
	intentRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{intentKey, markerKey}, Revision: revision},
	)
	if err != nil {
		return false, false, err
	}
	if intentRead == nil || len(intentRead.Values) != 2 {
		return false, false, corruptHierarchyDeletion()
	}
	if intentRead.Values[1] != nil {
		return false, false, nil
	}
	if intentRead.Values[0] == nil {
		return false, true, nil
	}
	var intent HierarchyDeletionIntent
	if decodeHierarchyDeletionRecord(intentRead.Values[0].Value, hierarchyDeletionLargeRecordBytes, &intent) != nil ||
		intent.OperationID != operationID {
		return false, false, corruptHierarchyDeletion()
	}
	tombstoneKey := HierarchyDeletionTombstoneKey(string(intent.TargetKind), intent.TargetID)
	tombstoneRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{tombstoneKey}, Revision: revision},
	)
	if err != nil {
		return false, false, err
	}
	if tombstoneRead == nil || len(tombstoneRead.Values) != 1 || tombstoneRead.Values[0] == nil {
		return false, false, corruptHierarchyDeletion()
	}
	var tombstone HierarchyDeletionTombstone
	if decodeHierarchyDeletionRecord(
		tombstoneRead.Values[0].Value,
		hierarchyDeletionLargeRecordBytes,
		&tombstone,
	) != nil ||
		tombstone.OperationID != operationID ||
		tombstone.Terminal == nil {
		return false, false, corruptHierarchyDeletion()
	}
	if tombstone.CurrentTaskID != task.ID {
		return false, true, nil
	}
	if tombstone.Terminal.RetainUntil.After(now) {
		return false, false, nil
	}
	if tombstone.Terminal.Status != string(TaskStatusCompleted) || tombstone.Phase != HierarchyDeletionRetained {
		transaction, transactErr := repository.store.Transact(ctx,
			[]etcdstore.Condition{
				{Key: taskKey(task.ID), ModRevision: taskRevision},
				{Key: retention.Key, ModRevision: retention.ModRevision},
				{Key: tombstoneKey, ModRevision: tombstoneRead.Values[0].ModRevision},
				{Key: markerKey},
			},
			[]etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: retention.Key}},
		)
		if transactErr != nil {
			return false, false, transactErr
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			return false, false, errs.New(errs.KindStateConflict, "hierarchy deletion failed Task prune fence changed")
		}
		return true, false, nil
	}
	pruneKey, _ := HierarchyDeletionPruneIntentKey(operationID)
	pruneRead, err := repository.store.Get(ctx, pruneKey)
	if err != nil {
		return false, false, err
	}
	if pruneRead.Entry == nil {
		prune := HierarchyDeletionPruneIntent{
			Schema: 1, ParentOperationID: operationID, Phase: HierarchyDeletionPruneReceipts,
			RetainUntil: tombstone.Terminal.RetainUntil, UpdatedAt: now,
		}
		value, encodeErr := encodeHierarchyDeletionRecord(prune, hierarchyDeletionSmallRecordBytes)
		if encodeErr != nil {
			return false, false, encodeErr
		}
		defer clear(value)
		transaction, transactErr := repository.store.Transact(ctx,
			[]etcdstore.Condition{
				{Key: pruneKey}, {Key: tombstoneKey, ModRevision: tombstoneRead.Values[0].ModRevision},
				{Key: taskKey(task.ID), ModRevision: taskRevision}, {Key: markerKey},
			},
			[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: pruneKey, Value: value}},
		)
		if transactErr != nil {
			return false, false, transactErr
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			return false, false, errs.New(errs.KindStateConflict, "hierarchy deletion prune initiation changed")
		}
		return true, false, nil
	}
	var prune HierarchyDeletionPruneIntent
	if decodeHierarchyDeletionRecord(pruneRead.Entry.Value, hierarchyDeletionSmallRecordBytes, &prune) != nil ||
		prune.ParentOperationID != operationID || !prune.RetainUntil.Equal(tombstone.Terminal.RetainUntil) {
		clear(pruneRead.Entry.Value)
		return false, false, corruptHierarchyDeletion()
	}
	clear(pruneRead.Entry.Value)
	changed, err := repository.advanceHierarchyDeletionPrune(
		ctx,
		task,
		tombstone,
		tombstoneRead.Values[0].ModRevision,
		prune,
		pruneRead.Entry.ModRevision,
		now,
	)
	return changed, false, err
}

func (repository *TaskRepository) advanceHierarchyDeletionPrune(
	ctx context.Context,
	task TaskRecord,
	tombstone HierarchyDeletionTombstone,
	tombstoneRevision int64,
	prune HierarchyDeletionPruneIntent,
	pruneRevision int64,
	now time.Time,
) (bool, error) {
	operationID := tombstone.OperationID
	prefixes, err := hierarchyDeletionPrunePrefixes(operationID, prune.Phase)
	if err != nil {
		return false, err
	}
	for _, prefix := range prefixes {
		page, rangeErr := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, Limit: int64(maximumTaskPruneBatchRecords + 1),
		})
		if rangeErr != nil {
			return false, rangeErr
		}
		if page == nil {
			return false, corruptHierarchyDeletion()
		}
		if len(page.Values) == 0 {
			continue
		}
		count := min(len(page.Values), maximumTaskPruneBatchRecords)
		conditions := []etcdstore.Condition{{Key: mustHierarchyDeletionPruneIntentKey(operationID), ModRevision: pruneRevision}}
		mutations := make([]etcdstore.Mutation, 0, count)
		for index := 0; index < count; index++ {
			conditions = append(
				conditions,
				etcdstore.Condition{Key: page.Values[index].Key, ModRevision: page.Values[index].ModRevision},
			)
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: page.Values[index].Key})
		}
		transaction, transactErr := repository.store.Transact(ctx, conditions, mutations)
		if transactErr != nil {
			return false, transactErr
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			return false, errs.New(errs.KindStateConflict, "hierarchy deletion prune batch changed")
		}
		return true, nil
	}
	if prune.Phase != HierarchyDeletionPruneFinal {
		prune.Phase = nextHierarchyDeletionPrunePhase(prune.Phase)
		prune.NextOrdinal = 0
		prune.UpdatedAt = now
		value, encodeErr := encodeHierarchyDeletionRecord(prune, hierarchyDeletionSmallRecordBytes)
		if encodeErr != nil {
			return false, encodeErr
		}
		defer clear(value)
		transaction, transactErr := repository.store.Transact(ctx,
			[]etcdstore.Condition{{Key: mustHierarchyDeletionPruneIntentKey(operationID), ModRevision: pruneRevision}},
			[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: mustHierarchyDeletionPruneIntentKey(operationID), Value: value}},
		)
		if transactErr != nil {
			return false, transactErr
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			return false, errs.New(errs.KindStateConflict, "hierarchy deletion prune phase changed")
		}
		return true, nil
	}
	return repository.finishHierarchyDeletionPrune(ctx, task, tombstone, tombstoneRevision, pruneRevision)
}

func hierarchyDeletionPrunePrefixes(operationID string, phase HierarchyDeletionPrunePhase) ([]string, error) {
	switch phase {
	case HierarchyDeletionPruneReceipts:
		receipts, err := HierarchyDeletionReceiptPrefix(operationID)
		if err != nil {
			return nil, err
		}
		successors, err := HierarchyDeletionSuccessorPrefix(operationID)
		return []string{receipts, successors}, err
	case HierarchyDeletionPruneProgress:
		prefix, err := HierarchyDeletionProgressPrefix(operationID)
		return []string{prefix}, err
	case HierarchyDeletionPruneChildren:
		prefix, err := HierarchyDeletionChildPrefix(operationID)
		return []string{prefix}, err
	case HierarchyDeletionPruneActions:
		prefix, err := HierarchyDeletionActionPrefix(operationID)
		return []string{prefix}, err
	case HierarchyDeletionPruneCompletions:
		prefix, err := HierarchyDeletionCompletionPrefix(operationID)
		return []string{prefix}, err
	case HierarchyDeletionPruneFinal:
		return nil, nil
	default:
		return nil, corruptHierarchyDeletion()
	}
}

func nextHierarchyDeletionPrunePhase(phase HierarchyDeletionPrunePhase) HierarchyDeletionPrunePhase {
	switch phase {
	case HierarchyDeletionPruneReceipts:
		return HierarchyDeletionPruneProgress
	case HierarchyDeletionPruneProgress:
		return HierarchyDeletionPruneChildren
	case HierarchyDeletionPruneChildren:
		return HierarchyDeletionPruneActions
	case HierarchyDeletionPruneActions:
		return HierarchyDeletionPruneCompletions
	case HierarchyDeletionPruneCompletions:
		return HierarchyDeletionPruneFinal
	default:
		return ""
	}
}

func (repository *TaskRepository) finishHierarchyDeletionPrune(
	ctx context.Context,
	task TaskRecord,
	tombstone HierarchyDeletionTombstone,
	tombstoneRevision int64,
	pruneRevision int64,
) (bool, error) {
	operationID := tombstone.OperationID
	keys := []string{
		HierarchyDeletionTombstoneKey(string(tombstone.TargetKind), tombstone.TargetID),
		mustHierarchyDeletionOperationKey(HierarchyDeletionCleanupFenceKey(operationID)),
		mustHierarchyDeletionOperationKey(HierarchyDeletionReplayTargetKey(operationID)),
		mustHierarchyDeletionOperationKey(HierarchyDeletionIntentKey(operationID)),
		HierarchyDeletionLockKey(string(tombstone.TargetKind), tombstone.TargetID),
		mustHierarchyDeletionOperationKey(HierarchyDeletionReceiptSummaryKey(operationID)),
		mustHierarchyDeletionOperationKey(HierarchyDeletionCompletionSummaryKey(operationID)),
		mustHierarchyDeletionOperationKey(HierarchyDeletionReceiptScanCursorKey(operationID, task.ID)),
		mustHierarchyDeletionOperationKey(HierarchyDeletionCompletionScanCursorKey(operationID, task.ID)),
		mustHierarchyDeletionPruneIntentKey(operationID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != len(keys) || read.Values[0] == nil ||
		read.Values[0].ModRevision != tombstoneRevision || read.Values[4] != nil ||
		read.Values[9] == nil || read.Values[9].ModRevision != pruneRevision {
		return false, corruptHierarchyDeletion()
	}
	conditions := make([]etcdstore.Condition, len(keys))
	mutations := make([]etcdstore.Mutation, 0, len(keys))
	for index, key := range keys {
		conditions[index] = etcdstore.Condition{Key: key, ModRevision: keyValueRevision(read.Values[index])}
		if read.Values[index] != nil {
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
		}
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "hierarchy deletion prune finalization changed")
	}
	return true, nil
}

func mustHierarchyDeletionPruneIntentKey(operationID string) string {
	key, _ := HierarchyDeletionPruneIntentKey(operationID)
	return key
}

func mustHierarchyDeletionOperationKey(key string, err error) string {
	if err != nil {
		return ""
	}
	return key
}
