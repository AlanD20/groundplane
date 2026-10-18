package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyPlanningRejection releases only an unsealed operation that has
// never published work. Its archived evidence outlives the target-scoped lock,
// so another removal can be accepted without rewriting the failed Task.
func (repository *HierarchyDeletionRepository) PrepareHierarchyPlanningRejection(ctx context.Context,
	task workqueue.TaskSnapshot, revision int64, reason hierarchydeletion.PlanningRejectionReason, at time.Time,
) (workqueue.Change, workexecution.Input, error) {
	outcome := workexecution.Failed
	if task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	}
	if task.Revision <= 0 || revision < task.Revision || task.Task.PublicationComplete || task.Task.ExpectedItems != 0 ||
		task.Task.Progress.Total() != 0 || task.Task.FinishedAt != nil || task.Task.FirstFailure != nil ||
		!reason.Valid(task.Task, outcome, at) || at.Before(task.Task.UpdatedAt) {
		return workqueue.Change{}, workexecution.Input{}, errs.New(errs.KindStateConflict, "removal planning has no matching rejection without published work")
	}
	op, err := repository.OperationByTaskAtRevision(ctx, task.Task.ID, revision)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	if op.Tombstone.Phase != hierarchydeletion.HierarchyDeletionPlanning || op.Tombstone.PlanCount != nil || op.Tombstone.PlanDigest != nil {
		return workqueue.Change{}, workexecution.Input{}, errs.New(errs.KindStateConflict, "removal already has an executable plan")
	}
	id, kind := op.Tombstone.TargetID, op.Tombstone.TargetKind
	rootKey := hierarchyDeletionPrimaryKey(kind, id)
	lockKey := hierarchydeletion.HierarchyDeletionLockKey(string(kind), id)
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(kind), id)
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(op.Tombstone.OperationID)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	keys := []string{rootKey, lockKey, hierarchydeletion.TaskBindingKey(task.Task.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return workqueue.Change{}, workexecution.Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return workqueue.Change{}, workexecution.Input{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	var lock hierarchydeletion.HierarchyDeletionLock
	if hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[1].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &lock) != nil ||
		lock.ParentOperationID != op.Tombstone.OperationID || lock.TargetID != id || lock.TargetKind != kind ||
		lock.DeletionEpoch != op.Tombstone.DeletionEpoch || lock.TombstoneKey != tombstoneKey || lock.CleanupFenceKey != fenceKey {
		return workqueue.Change{}, workexecution.Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	root, err := releaseHierarchyPlanningRoot(kind, id, task.Task.ID, read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	op.Fence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchClosed
	op.Fence.UpdatedAt, op.UpdatedAt = at, at
	raw, err := hierarchydeletion.EncodePlanningRejection(hierarchydeletion.PlanningRejection{Operation: op, Reason: reason, Outcome: outcome, At: at}, task.Task)
	if err != nil {
		clear(root)
		return workqueue.Change{}, workexecution.Input{}, err
	}
	evidence := hierarchydeletion.PlanningRejectionEvidence(task.Task, raw)
	failureKey := hierarchydeletion.PlanningRejectionKey(task.Task.ID)
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: workqueue.TaskItemsPrefix(task.Task.ID), Prefix: true},
		{Key: tombstoneKey, ModRevision: op.TombstoneRevision},
		{Key: fenceKey, ModRevision: op.FenceRevision}, {Key: failureKey},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: rootKey, Value: root},
		{Type: keyvalue.MutationPut, Key: failureKey, Value: raw},
		{Type: keyvalue.MutationDelete, Key: lockKey},
		{Type: keyvalue.MutationDelete, Key: tombstoneKey},
		{Type: keyvalue.MutationDelete, Key: fenceKey},
	}}
	for _, value := range read.Values {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	return change, evidence, nil
}
