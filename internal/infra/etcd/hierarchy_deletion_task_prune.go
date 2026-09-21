package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(*task.idempotencyMarker)
	if err != nil {
		return false, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	operationID := task.Params[taskjournal.TaskHierarchyDeletionOperationParam]
	if !hierarchydeletion.ValidHierarchyDeletionPrivateID(operationID, "del") {
		return false, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	intentKey, _ := hierarchydeletion.HierarchyDeletionIntentKey(operationID)
	intentRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{intentKey, markerKey}, Revision: revision},
	)
	if err != nil {
		return false, false, err
	}
	if intentRead == nil || len(intentRead.Values) != 2 {
		return false, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if intentRead.Values[1] != nil {
		return false, false, nil
	}
	if intentRead.Values[0] == nil {
		return false, true, nil
	}
	var intent hierarchydeletion.HierarchyDeletionIntent
	if hierarchydeletion.DecodeHierarchyDeletionRecord(intentRead.Values[0].Value, hierarchydeletion.HierarchyDeletionLargeRecordBytes, &intent) != nil ||
		intent.OperationID != operationID {
		return false, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(intent.TargetKind), intent.TargetID)
	tombstoneRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{tombstoneKey}, Revision: revision},
	)
	if err != nil {
		return false, false, err
	}
	if tombstoneRead == nil || len(tombstoneRead.Values) != 1 || tombstoneRead.Values[0] == nil {
		return false, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var tombstone hierarchydeletion.HierarchyDeletionTombstone
	if hierarchydeletion.DecodeHierarchyDeletionRecord(
		tombstoneRead.Values[0].Value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		&tombstone,
	) != nil ||
		tombstone.OperationID != operationID ||
		tombstone.Terminal == nil {
		return false, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if tombstone.CurrentTaskID != task.ID {
		return false, true, nil
	}
	if tombstone.Terminal.RetainUntil.After(now) {
		return false, false, nil
	}
	if tombstone.Terminal.Status != string(taskjournal.TaskStatusCompleted) || tombstone.Phase != hierarchydeletion.HierarchyDeletionRetained {
		transaction, transactErr := repository.store.Transact(ctx,
			[]etcdstore.Condition{
				{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
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
	pruneKey, _ := hierarchydeletion.HierarchyDeletionPruneIntentKey(operationID)
	pruneRead, err := repository.store.Get(ctx, pruneKey)
	if err != nil {
		return false, false, err
	}
	if pruneRead.Entry == nil {
		prune := hierarchydeletion.HierarchyDeletionPruneIntent{
			Schema: 1, ParentOperationID: operationID, Phase: hierarchydeletion.HierarchyDeletionPruneReceipts,
			RetainUntil: tombstone.Terminal.RetainUntil, UpdatedAt: now,
		}
		value, encodeErr := hierarchydeletion.EncodeHierarchyDeletionRecord(prune, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
		if encodeErr != nil {
			return false, false, encodeErr
		}
		defer clear(value)
		transaction, transactErr := repository.store.Transact(ctx,
			[]etcdstore.Condition{
				{Key: pruneKey}, {Key: tombstoneKey, ModRevision: tombstoneRead.Values[0].ModRevision},
				{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision}, {Key: markerKey},
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
	var prune hierarchydeletion.HierarchyDeletionPruneIntent
	if hierarchydeletion.DecodeHierarchyDeletionRecord(pruneRead.Entry.Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &prune) != nil ||
		prune.ParentOperationID != operationID || !prune.RetainUntil.Equal(tombstone.Terminal.RetainUntil) {
		clear(pruneRead.Entry.Value)
		return false, false, hierarchydeletion.CorruptHierarchyDeletion()
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
	tombstone hierarchydeletion.HierarchyDeletionTombstone,
	tombstoneRevision int64,
	prune hierarchydeletion.HierarchyDeletionPruneIntent,
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
			return false, hierarchydeletion.CorruptHierarchyDeletion()
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
	if prune.Phase != hierarchydeletion.HierarchyDeletionPruneFinal {
		prune.Phase = nextHierarchyDeletionPrunePhase(prune.Phase)
		prune.NextOrdinal = 0
		prune.UpdatedAt = now
		value, encodeErr := hierarchydeletion.EncodeHierarchyDeletionRecord(prune, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
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

func hierarchyDeletionPrunePrefixes(operationID string, phase hierarchydeletion.HierarchyDeletionPrunePhase) ([]string, error) {
	switch phase {
	case hierarchydeletion.HierarchyDeletionPruneReceipts:
		receipts, err := hierarchydeletion.HierarchyDeletionReceiptPrefix(operationID)
		if err != nil {
			return nil, err
		}
		successors, err := hierarchydeletion.HierarchyDeletionSuccessorPrefix(operationID)
		return []string{receipts, successors}, err
	case hierarchydeletion.HierarchyDeletionPruneProgress:
		prefix, err := hierarchydeletion.HierarchyDeletionProgressPrefix(operationID)
		return []string{prefix}, err
	case hierarchydeletion.HierarchyDeletionPruneChildren:
		prefix, err := hierarchydeletion.HierarchyDeletionChildPrefix(operationID)
		return []string{prefix}, err
	case hierarchydeletion.HierarchyDeletionPruneActions:
		prefix, err := hierarchydeletion.HierarchyDeletionActionPrefix(operationID)
		return []string{prefix}, err
	case hierarchydeletion.HierarchyDeletionPruneCompletions:
		prefix, err := hierarchydeletion.HierarchyDeletionCompletionPrefix(operationID)
		return []string{prefix}, err
	case hierarchydeletion.HierarchyDeletionPruneFinal:
		return nil, nil
	default:
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
}

func nextHierarchyDeletionPrunePhase(phase hierarchydeletion.HierarchyDeletionPrunePhase) hierarchydeletion.HierarchyDeletionPrunePhase {
	switch phase {
	case hierarchydeletion.HierarchyDeletionPruneReceipts:
		return hierarchydeletion.HierarchyDeletionPruneProgress
	case hierarchydeletion.HierarchyDeletionPruneProgress:
		return hierarchydeletion.HierarchyDeletionPruneChildren
	case hierarchydeletion.HierarchyDeletionPruneChildren:
		return hierarchydeletion.HierarchyDeletionPruneActions
	case hierarchydeletion.HierarchyDeletionPruneActions:
		return hierarchydeletion.HierarchyDeletionPruneCompletions
	case hierarchydeletion.HierarchyDeletionPruneCompletions:
		return hierarchydeletion.HierarchyDeletionPruneFinal
	default:
		return ""
	}
}

func (repository *TaskRepository) finishHierarchyDeletionPrune(
	ctx context.Context,
	task TaskRecord,
	tombstone hierarchydeletion.HierarchyDeletionTombstone,
	tombstoneRevision int64,
	pruneRevision int64,
) (bool, error) {
	operationID := tombstone.OperationID
	keys := []string{
		hierarchydeletion.HierarchyDeletionTombstoneKey(string(tombstone.TargetKind), tombstone.TargetID),
		mustHierarchyDeletionOperationKey(hierarchydeletion.HierarchyDeletionCleanupFenceKey(operationID)),
		mustHierarchyDeletionOperationKey(hierarchydeletion.HierarchyDeletionReplayTargetKey(operationID)),
		mustHierarchyDeletionOperationKey(hierarchydeletion.HierarchyDeletionIntentKey(operationID)),
		hierarchydeletion.HierarchyDeletionLockKey(string(tombstone.TargetKind), tombstone.TargetID),
		mustHierarchyDeletionOperationKey(hierarchydeletion.HierarchyDeletionReceiptSummaryKey(operationID)),
		mustHierarchyDeletionOperationKey(hierarchydeletion.HierarchyDeletionCompletionSummaryKey(operationID)),
		mustHierarchyDeletionOperationKey(hierarchydeletion.HierarchyDeletionReceiptScanCursorKey(operationID, task.ID)),
		mustHierarchyDeletionOperationKey(hierarchydeletion.HierarchyDeletionCompletionScanCursorKey(operationID, task.ID)),
		mustHierarchyDeletionPruneIntentKey(operationID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != len(keys) || read.Values[0] == nil ||
		read.Values[0].ModRevision != tombstoneRevision || read.Values[4] != nil ||
		read.Values[9] == nil || read.Values[9].ModRevision != pruneRevision {
		return false, hierarchydeletion.CorruptHierarchyDeletion()
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
	key, _ := hierarchydeletion.HierarchyDeletionPruneIntentKey(operationID)
	return key
}

func mustHierarchyDeletionOperationKey(key string, err error) string {
	if err != nil {
		return ""
	}
	return key
}
