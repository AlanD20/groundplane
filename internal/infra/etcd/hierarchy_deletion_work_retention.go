package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionbackupcleanup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyControllerRetention removes only metadata-action evidence.
// An unfinished removal retains its sealed plan: successful sibling actions must
// still be identifiable if the operator resumes the same removal later.
func (repository *HierarchyDeletionRepository) PrepareHierarchyControllerRetention(ctx context.Context, view workqueue.View, at time.Time) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionRemove ||
		(item.Operation != workexecution.Hierarchy && item.Operation != workexecution.RemovalBackupObject) || item.Placement.Executor != workexecution.Controller ||
		item.TaskID != task.ID || item.Phase != workexecution.Settled || item.Result == nil ||
		(item.Operation == workexecution.Hierarchy && item.AttemptNumber != 0) ||
		(item.Operation == workexecution.RemovalBackupObject && item.AttemptNumber == 0) || item.ActiveAttemptID != "" || task.CollectionStartedAt == nil ||
		task.RetainUntil == nil || at.Before(*task.RetainUntil) || view.ReadRevision <= 0 {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "removal metadata is not eligible for retention")
	}
	if task.Outcome != workexecution.Succeeded {
		return false, workqueue.Change{}, nil
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	operation := captured.Operation
	if operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionRetained || operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchClosed {
		return false, workqueue.Change{}, nil
	}
	completionKey, err := hierarchydeletion.HierarchyDeletionCompletionKey(captured.Action.ParentOperationID, captured.Action.Ordinal)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	summaryKey := hierarchydeletion.WorkSummaryKey(task.ID)
	lockKey := hierarchydeletion.HierarchyDeletionLockKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{summaryKey, completionKey, lockKey}, Revision: view.ReadRevision})
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 3 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] != nil {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	summary, err := hierarchydeletion.DecodeWorkSummary(read.Values[0].Value)
	if err != nil || summary.TaskID != task.ID || summary.OperationID != operation.Tombstone.OperationID ||
		summary.Count != task.ExpectedItems || summary.Outcome != task.Outcome || task.FinishedAt == nil ||
		!summary.CompletedAt.Equal(*task.FinishedAt) || operation.Tombstone.PlanDigest == nil ||
		summary.PlanDigest != *operation.Tombstone.PlanDigest || !hierarchydeletion.ValidHierarchyDeletionDigest(summary.ResultDigest) ||
		item.Outcome != workexecution.Succeeded || item.Result.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "hierarchy-result") ||
		item.Result.SHA256 != hierarchydeletion.HierarchyDeletionBytesDigest(read.Values[1].Value) {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	actionKey, err := hierarchydeletion.HierarchyDeletionActionKey(captured.Action.ParentOperationID, captured.Action.Ordinal)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(captured.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		keyvalue.Condition{Key: summaryKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: completionKey, ModRevision: read.Values[1].ModRevision}, keyvalue.Condition{Key: lockKey})}
	for _, key := range []string{actionKey, completionKey, hierarchydeletion.WorkBindingKey(item.ID), hierarchydeletion.WorkStopKey(item.ID)} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Prefix: true,
		Key: hierarchydeletion.WorkStopEvidencePrefix(item.ID)})
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionRecoveryPointRemove ||
		captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionOrphanObjectRemove {
		conditions, mutations, err := hierarchydeletionbackupcleanup.NewRepository(repository.store).
			PrepareRetention(ctx, operation, captured.Action, view.ReadRevision)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, conditions...)
		change.Mutations = append(change.Mutations, mutations...)
	}
	return true, change, nil
}
