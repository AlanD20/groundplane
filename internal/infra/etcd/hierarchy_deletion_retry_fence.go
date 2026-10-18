package etcd

import (
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func prepareHierarchyRetryFence(task workqueue.TaskSnapshot, operation hierarchydeletion.HierarchyDeletionOperation,
	summaryValue *keyvalue.KeyValue, at time.Time,
) (workqueue.Change, error) {
	key := hierarchydeletion.WorkSummaryKey(task.Task.ID)
	if operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting || operation.Fence.Generation == math.MaxInt64 {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if task.Task.FinishedAt == nil {
		if summaryValue != nil || operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchOpen {
			return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}}}, nil
	}
	if summaryValue == nil || operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchClosed {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	summary, err := hierarchydeletion.DecodeWorkSummary(summaryValue.Value)
	if err != nil || summary.TaskID != task.Task.ID || summary.OperationID != operation.Tombstone.OperationID ||
		summary.Outcome != task.Task.Outcome || !summary.CompletedAt.Equal(*task.Task.FinishedAt) ||
		summary.Count != task.Task.ExpectedItems || operation.Tombstone.PlanDigest == nil || summary.PlanDigest != *operation.Tombstone.PlanDigest {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	fence := operation.Fence
	fence.Dispatch, fence.Generation, fence.UpdatedAt = hierarchydeletion.HierarchyDeletionDispatchOpen, fence.Generation+1, at
	value, err := hierarchydeletion.EncodeHierarchyDeletionRecord(fence, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		return workqueue.Change{}, err
	}
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: summaryValue.ModRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}, {Type: keyvalue.MutationPut, Key: fenceKey, Value: value}}}, nil
}
