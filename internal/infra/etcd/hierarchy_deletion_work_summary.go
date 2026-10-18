package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyWorkSummary(ctx context.Context, finished workqueue.FinishedWork, at time.Time) (hierarchydeletion.HierarchyDeletionOperation, hierarchydeletion.WorkSummary, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionRemove || finished.ReadRevision <= 0 || finished.Task.Revision <= 0 ||
		len(finished.Items) == 0 || uint64(len(finished.Items)) != uint64(task.ExpectedItems) || at.Before(task.UpdatedAt) {
		return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.WorkSummary{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	operation, err := repository.OperationByTaskAtRevision(ctx, task.ID, finished.ReadRevision)
	if err != nil {
		return operation, hierarchydeletion.WorkSummary{}, err
	}
	if operation.Tombstone.PlanCount == nil || *operation.Tombstone.PlanCount != int64(len(finished.Items)) || operation.Tombstone.PlanDigest == nil {
		return operation, hierarchydeletion.WorkSummary{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	outcome, err := workexecution.CompletionOutcome(task)
	if err != nil {
		return operation, hierarchydeletion.WorkSummary{}, err
	}
	results := make([]string, len(finished.Items))
	for _, snapshot := range finished.Items {
		item := snapshot.Item
		if item.Phase != workexecution.Settled || item.Result == nil || item.ActiveAttemptID != "" || item.Result.Validate() != nil {
			return operation, hierarchydeletion.WorkSummary{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		captured, err := repository.LoadHierarchyWork(ctx, workqueue.View{Task: finished.Task, Item: snapshot, ReadRevision: finished.ReadRevision})
		if err != nil {
			return operation, hierarchydeletion.WorkSummary{}, err
		}
		ordinal := captured.Action.Ordinal
		if ordinal < 0 || ordinal >= int64(len(results)) || results[ordinal] != "" {
			return operation, hierarchydeletion.WorkSummary{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		results[ordinal] = hierarchydeletion.HierarchyDeletionFoldDigest("hierarchy-work-result", item.ID, item.Input.SHA256, string(item.Outcome), item.Result.ID, item.Result.SHA256)
	}
	digest := *operation.Tombstone.PlanDigest
	for _, result := range results {
		if result == "" {
			return operation, hierarchydeletion.WorkSummary{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		digest = hierarchydeletion.HierarchyDeletionFoldDigest("hierarchy-work-summary", digest, result)
	}
	return operation, hierarchydeletion.WorkSummary{TaskID: task.ID, OperationID: operation.Tombstone.OperationID,
		PlanDigest: *operation.Tombstone.PlanDigest, ResultDigest: digest, Count: task.ExpectedItems,
		Outcome: outcome, CompletedAt: at}, nil
}

// PrepareHierarchyWorkFinish joins aggregate removal evidence with Task finish.
// Task revision guards every work transition, keeping transaction size bounded
// independently of the number of resources in the captured graph.
func (repository *HierarchyDeletionRepository) PrepareHierarchyWorkFinish(ctx context.Context, finished workqueue.FinishedWork, at time.Time) (workqueue.Change, error) {
	operation, summary, err := repository.prepareHierarchyWorkSummary(ctx, finished, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := hierarchydeletion.EncodeWorkSummary(summary)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := hierarchydeletion.WorkSummaryKey(summary.TaskID)
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(summary.OperationID)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	fence := operation.Fence
	if summary.Outcome == workexecution.Succeeded {
		fence.Phase = hierarchydeletion.HierarchyDeletionRetained
	}
	fence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchClosed
	fence.Generation++
	fence.UpdatedAt = at
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(fence, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change := workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: key}, {Key: workqueue.TaskKey(summary.TaskID), ModRevision: finished.Task.Revision},
			{Key: fenceKey, ModRevision: operation.FenceRevision},
			{Key: hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID), ModRevision: operation.TombstoneRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}, {Type: keyvalue.MutationPut, Key: fenceKey, Value: fenceValue}},
	}
	if summary.Outcome == workexecution.Succeeded {
		root, err := repository.prepareHierarchyRemovedRoot(ctx, operation, finished.ReadRevision)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, root.Conditions...)
		change.Mutations = append(change.Mutations, root.Mutations...)
	}
	return change, nil
}
