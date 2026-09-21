package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strconv"
	"time"
)

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
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
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
