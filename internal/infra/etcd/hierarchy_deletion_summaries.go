package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"
	"time"
)

func (repository *HierarchyDeletionRepository) buildHierarchyDeletionSummaries(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	rootCompletion []byte,
	finalCheckpoint string,
	completedAt time.Time,
	revision int64,
) (hierarchyDeletionRootAckChange, error) {
	count := *operation.Tombstone.PlanCount
	completionDigest := hierarchydeletion.HierarchyDeletionFoldDigest("gp-deletion-completion-set-v1", operation.Tombstone.OperationID)
	receiptDigest := hierarchydeletion.HierarchyDeletionFoldDigest("gp-deletion-receipt-set-v1", operation.Tombstone.OperationID)
	childCount := int64(0)
	for begin := int64(0); begin < count-1; begin += hierarchydeletion.PlanBatchSize {
		end := min(begin+hierarchydeletion.PlanBatchSize, count-1)
		keys := make([]string, end-begin)
		for ordinal := begin; ordinal < end; ordinal++ {
			keys[ordinal-begin] = mustHierarchyDeletionCompletionKey(operation.Tombstone.OperationID, ordinal)
		}
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
		if err != nil {
			return hierarchyDeletionRootAckChange{}, err
		}
		if read == nil || len(read.Values) != len(keys) {
			return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for offset, value := range read.Values {
			ordinal := begin + int64(offset)
			if value == nil {
				return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			var completion hierarchydeletion.HierarchyDeletionActionCompletion
			if hierarchydeletion.DecodeHierarchyDeletionRecord(value.Value, hierarchydeletion.HierarchyDeletionCompletionRecordBytes, &completion) != nil ||
				completion.ParentOperationID != operation.Tombstone.OperationID ||
				completion.DeletionEpoch != operation.Tombstone.DeletionEpoch || completion.Ordinal != ordinal {
				return hierarchyDeletionRootAckChange{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			valueDigest := hierarchydeletion.HierarchyDeletionBytesDigest(value.Value)
			completionDigest = hierarchydeletion.HierarchyDeletionFoldDigest(
				"gp-deletion-completion-set-item-v1",
				completionDigest,
				strconv.FormatInt(ordinal, 10),
				valueDigest,
			)
			if completion.AgentProof != nil {
				childCount++
				receiptDigest = hierarchydeletion.HierarchyDeletionFoldDigest(
					"gp-deletion-receipt-set-item-v1",
					receiptDigest,
					strconv.FormatInt(ordinal, 10),
					completion.AgentProof.ReceiptDigest,
				)
			}
		}
	}
	completionDigest = hierarchydeletion.HierarchyDeletionFoldDigest(
		"gp-deletion-completion-set-item-v1",
		completionDigest,
		strconv.FormatInt(count-1, 10),
		hierarchydeletion.HierarchyDeletionBytesDigest(rootCompletion),
	)
	receiptSummary := hierarchydeletion.HierarchyDeletionReceiptSummary{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		DeletionEpoch: operation.Tombstone.DeletionEpoch, ChildCount: childCount,
		OrderedReceiptSetDigest: receiptDigest, FinalCheckpointDigest: finalCheckpoint, CompletedAt: completedAt,
	}
	receiptSummaryValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(receiptSummary, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	completionSummary := hierarchydeletion.HierarchyDeletionCompletionSummary{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		DeletionEpoch: operation.Tombstone.DeletionEpoch, PlanCount: count, PlanDigest: *operation.Tombstone.PlanDigest,
		OrderedCompletionSetDigest: completionDigest,
		AgentReceiptSummaryDigest:  hierarchydeletion.HierarchyDeletionBytesDigest(receiptSummaryValue),
		FinalCheckpointDigest:      finalCheckpoint, CompletedAt: completedAt, RetainUntil: completedAt.Add(taskjournal.TaskRetention),
	}
	completionSummaryValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(completionSummary, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(receiptSummaryValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	receiptCursor := hierarchydeletion.HierarchyDeletionScanCursor{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		TaskID: operation.Tombstone.CurrentTaskID, NextOrdinal: count, Count: childCount,
		OrderedSetDigest: receiptDigest, UpdatedAt: completedAt,
	}
	completionCursor := hierarchydeletion.HierarchyDeletionScanCursor{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		TaskID: operation.Tombstone.CurrentTaskID, NextOrdinal: count, Count: count,
		OrderedSetDigest: completionDigest, UpdatedAt: completedAt,
	}
	receiptCursorValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(receiptCursor, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(receiptSummaryValue)
		clear(completionSummaryValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	completionCursorValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(completionCursor, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(receiptSummaryValue)
		clear(completionSummaryValue)
		clear(receiptCursorValue)
		return hierarchyDeletionRootAckChange{}, err
	}
	receiptSummaryKey, _ := hierarchydeletion.HierarchyDeletionReceiptSummaryKey(operation.Tombstone.OperationID)
	completionSummaryKey, _ := hierarchydeletion.HierarchyDeletionCompletionSummaryKey(operation.Tombstone.OperationID)
	receiptCursorKey, _ := hierarchydeletion.HierarchyDeletionReceiptScanCursorKey(
		operation.Tombstone.OperationID,
		operation.Tombstone.CurrentTaskID,
	)
	completionCursorKey, _ := hierarchydeletion.HierarchyDeletionCompletionScanCursorKey(
		operation.Tombstone.OperationID,
		operation.Tombstone.CurrentTaskID,
	)
	return hierarchyDeletionRootAckChange{
		conditions: []etcdstore.Condition{
			{Key: receiptSummaryKey},
			{Key: completionSummaryKey},
			{Key: receiptCursorKey},
			{Key: completionCursorKey},
			{Key: mustHierarchyDeletionCompletionKey(operation.Tombstone.OperationID, count-1)},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: receiptSummaryKey, Value: receiptSummaryValue},
			{Type: etcdstore.MutationPut, Key: completionSummaryKey, Value: completionSummaryValue},
			{Type: etcdstore.MutationPut, Key: receiptCursorKey, Value: receiptCursorValue},
			{Type: etcdstore.MutationPut, Key: completionCursorKey, Value: completionCursorValue},
		},
		values: [][]byte{receiptSummaryValue, completionSummaryValue, receiptCursorValue, completionCursorValue},
	}, nil
}
