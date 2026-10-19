package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type HierarchyCollection struct {
	Present           bool
	Eligible          bool
	PlanningRejection bool
	Operation         hierarchydeletion.HierarchyDeletionOperation
	Authority         workqueue.Change
}

// HierarchyCollectionGuard runs before BeginCollection freezes retries. A
// partially removed hierarchy retains both its Task and its captured plan.
func (repository *HierarchyDeletionRepository) HierarchyCollectionGuard(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
	at time.Time,
) (HierarchyCollection, error) {
	if task.Task.Action != workexecution.ActionRemove {
		return HierarchyCollection{}, nil
	}
	if task.Revision <= 0 || revision < task.Revision || task.Task.Validate() != nil {
		return HierarchyCollection{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	bindingKey, summaryKey := hierarchydeletion.TaskBindingKey(
		task.Task.ID,
	), hierarchydeletion.WorkSummaryKey(
		task.Task.ID,
	)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{bindingKey, summaryKey}, Revision: revision},
	)
	if err != nil {
		return HierarchyCollection{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return HierarchyCollection{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		if read.Values[1] != nil ||
			task.Task.Intent.ID == ids.DeriveAt(ids.KindPlan, task.Task.CreatedAt, task.Task.ID, "hierarchy-removal") {
			return HierarchyCollection{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		return HierarchyCollection{}, nil
	}
	guard := HierarchyCollection{Present: true}
	if task.Task.ExpectedItems == 0 && task.Task.Outcome != workexecution.Succeeded && task.Task.FinishedAt != nil {
		return repository.hierarchyPlanningCollectionGuard(ctx, task, revision, at, read.Values[0], read.Values[1])
	}
	if task.Task.Outcome != workexecution.Succeeded || task.Task.FinishedAt == nil || task.Task.RetainUntil == nil ||
		at.Before(*task.Task.RetainUntil) {
		return guard, nil
	}
	operation, err := repository.OperationByTaskAtRevision(ctx, task.Task.ID, revision)
	if err != nil {
		return guard, err
	}
	if read.Values[1] == nil || operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionRetained ||
		operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchClosed {
		return guard, hierarchydeletion.CorruptHierarchyDeletion()
	}
	summary, err := hierarchydeletion.DecodeWorkSummary(read.Values[1].Value)
	if err != nil || summary.TaskID != task.Task.ID || summary.OperationID != operation.Tombstone.OperationID ||
		summary.Outcome != task.Task.Outcome ||
		!summary.CompletedAt.Equal(*task.Task.FinishedAt) ||
		summary.Count != task.Task.ExpectedItems ||
		operation.Tombstone.PlanDigest == nil ||
		summary.PlanDigest != *operation.Tombstone.PlanDigest {
		return guard, hierarchydeletion.CorruptHierarchyDeletion()
	}
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(summary.OperationID)
	if err != nil {
		return guard, err
	}
	guard.Eligible, guard.Operation = true, operation
	guard.Authority.Conditions = []keyvalue.Condition{
		{
			Key:         bindingKey,
			ModRevision: read.Values[0].ModRevision,
		}, {Key: summaryKey, ModRevision: read.Values[1].ModRevision},
		{
			Key:         workqueue.TaskKey(task.Task.ID),
			ModRevision: task.Revision,
		}, {Key: fenceKey, ModRevision: operation.FenceRevision},
		{
			Key: hierarchydeletion.HierarchyDeletionTombstoneKey(
				string(operation.Tombstone.TargetKind),
				operation.Tombstone.TargetID,
			),
			ModRevision: operation.TombstoneRevision,
		},
		{
			Key: hierarchydeletion.HierarchyDeletionLockKey(
				string(operation.Tombstone.TargetKind),
				operation.Tombstone.TargetID,
			),
		},
		{Key: hierarchyDeletionPrimaryKey(operation.Tombstone.TargetKind, operation.Tombstone.TargetID)},
	}
	return guard, nil
}
