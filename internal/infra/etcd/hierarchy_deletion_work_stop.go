package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyUnstartedStop settles queued work before another execution
// attempt starts. Earlier completed removal actions remain completed;
// cancellation does not claim to restore deleted resources.
func (repository *HierarchyDeletionRepository) PrepareHierarchyUnstartedStop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	outcome workexecution.Outcome,
	at time.Time,
) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	stopped := task.Task.Stop != nil && task.Task.Stop.Outcome == outcome
	expired := outcome == workexecution.Failed && !at.Before(item.Item.Deadline)
	if task.Task.Action != workexecution.ActionRemove ||
		(item.Item.Operation != workexecution.Hierarchy && item.Item.Operation != workexecution.RemovalBackupObject) ||
		item.Item.TaskID != task.Task.ID ||
		task.Revision <= 0 ||
		(!stopped && !expired) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal work has no matching stop or deadline")
	}
	drained, cleanupConditions, err := repository.abandonUnstartedAttachPins(ctx, task, item)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !drained {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"unpublished Attach Secret preparation cleanup is still progressing",
		)
	}
	value, err := recordcodec.Encode("hierarchy-work-stopped", struct {
		TaskID  string                `json:"task_id"`
		WorkID  string                `json:"work_id"`
		Outcome workexecution.Outcome `json:"outcome"`
		At      time.Time             `json:"at"`
	}{task.Task.ID, item.Item.ID, outcome, at})
	if err != nil {
		return workqueue.Change{}, err
	}
	result := workexecution.Completion{
		Outcome: outcome, EffectsSettled: true,
		Evidence: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, task.Task.CreatedAt, item.Item.ID, "hierarchy-stop"),
			SHA256: hierarchydeletion.HierarchyDeletionBytesDigest(value),
		},
	}
	var change workqueue.Change
	if item.Item.AttemptNumber == 0 {
		change, err = workqueue.PrepareUnstartedSettlement(task, item, result, at)
	} else {
		change, err = workqueue.PrepareQueuedRetrySettlement(task, item, result, at)
	}
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key := hierarchydeletion.WorkStopKey(item.Item.ID)
	change.Conditions = append(change.Conditions, cleanupConditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
	)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
