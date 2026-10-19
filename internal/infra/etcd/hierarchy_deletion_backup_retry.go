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

// PrepareHierarchyBackupRetry preserves captured input and original failure.
// Uncertain effects get an explicit cleanup attempt; successful cleanup retains
// the original failure until the operator retries forward finalization.
func (repository *HierarchyDeletionRepository) PrepareHierarchyBackupRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if item.AttemptNumber == 0 {
		return repository.PrepareHierarchyUnstartedRetry(ctx, view, at)
	}
	if task.Action != workexecution.ActionRemove || task.CollectionStartedAt != nil ||
		item.Operation != workexecution.RemovalBackupObject || item.Placement.Executor != workexecution.Controller ||
		item.Input == nil || item.Result == nil || item.ActiveAttemptID != "" ||
		attempt.FinishedAt == nil || attempt.Result == nil ||
		attempt.Input != *item.Input || attempt.Number != item.AttemptNumber ||
		attempt.TaskID != task.ID || attempt.WorkItemID != item.ID || previous.Revision <= 0 {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"backup removal requires its saved terminal attempt",
		)
	}
	basis := item.Result
	if attempt.Direction == workexecution.Recovery && item.Cleanup != workexecution.CleanupNone {
		basis = item.CleanupResult
	}
	if basis == nil || *basis != *attempt.Result {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	failureKey, summaryKey := HierarchyBackupResultKey(attempt.ID), hierarchydeletion.WorkSummaryKey(task.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{failureKey, summaryKey}, Revision: view.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 2 || read.Values[0] == nil {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	failure, err := recordcodec.Decode[hierarchyBackupResult](read.Values[0].Value, "hierarchy-backup-result")
	if err != nil || len(read.Values[0].Value) > 4096 || failure.AttemptID != attempt.ID ||
		failure.Input != attempt.Input ||
		!failure.RecordedAt.Equal(*attempt.FinishedAt) ||
		attempt.Result.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, attempt.ID, "backup-removal-result") ||
		attempt.Result.SHA256 != hierarchydeletion.HierarchyDeletionBytesDigest(read.Values[0].Value) {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	direction := workexecution.Forward
	switch {
	case item.Phase == workexecution.Attention &&
		(item.Cleanup == workexecution.CleanupRequired || item.Cleanup == workexecution.CleanupStopped):
		if !failure.DeleteGranted || failure.Absent || attempt.Outcome == workexecution.Succeeded ||
			(failure.Reason != "object_absence_unconfirmed" && failure.Reason != "execution_not_started") {
			return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		direction = workexecution.Recovery
	case item.Phase == workexecution.Settled && item.Outcome == workexecution.Failed:
		if task.Stop != nil || item.ForwardRevoked {
			return workqueue.Change{}, errs.New(
				errs.KindTaskNotRetryable,
				"cancelled backup removal cannot resume forward",
			)
		}
		if attempt.Direction == workexecution.Recovery {
			confirmed := attempt.Outcome == workexecution.Succeeded && failure.Reason == "object_absence_confirmed"
			alreadyAbsent := attempt.Outcome == workexecution.Failed && failure.Reason == "execution_not_started"
			if (item.Cleanup != workexecution.CleanupComplete && item.Cleanup != workexecution.CleanupNone) ||
				!failure.Absent || (!confirmed && !alreadyAbsent) {
				return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
		} else if item.Cleanup != workexecution.CleanupNone || attempt.Outcome != workexecution.Failed ||
			(failure.DeleteGranted && !failure.Absent) ||
			(failure.Reason != "execution_not_started" && failure.Reason != "delete_not_authorized") {
			return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
	default:
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "backup removal has no retryable failure")
	}
	fence, err := prepareHierarchyRetryFence(view.Task, captured.Operation, read.Values[1], at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: direction, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(30 * time.Second),
	}, at)
	if err != nil {
		keyvalue.ClearMutationValues(fence.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, captured.Conditions...)
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: failureKey, ModRevision: read.Values[0].ModRevision},
	)
	change.Mutations = append(change.Mutations, fence.Mutations...)
	return change, nil
}
