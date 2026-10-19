package attachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Reports) PrepareWorkRetry(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot,
	revision int64,
	at time.Time,
) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.Attach || item.Item.Placement.Executor != workexecution.Agent ||
		item.Item.AttemptNumber == 0 {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Attach Retry requires a captured execution attempt",
		)
	}
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	if item.Item.Phase == workexecution.Attention {
		return owner.inputs.repository.PrepareRecoveryRetry(ctx, view, previous, at)
	}
	return owner.inputs.repository.PrepareForwardRetry(ctx, view, previous, at)
}

func (owner *Reports) stopPrepared(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	item, task := view.Item.Item, view.Task.Task
	if item.AttemptNumber == 0 {
		return owner.inputs.repository.PrepareUnstarted(ctx, view, at)
	}
	if item.ActiveAttemptID != "" || item.Input == nil || item.Cleanup != workexecution.CleanupNone {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Attach queued Retry still owns unresolved execution",
		)
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach queued Retry has not stopped or expired")
	}
	_, previous, err := owner.queue.ReadAttempt(ctx, task.ID, item.ID, item.AttemptNumber, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	attempt := previous.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Attach queued Retry lost its previous execution result",
		)
	}
	binding, err := owner.inputs.repository.PrepareFailedBinding(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *attempt.Result, EffectsSettled: true,
	}, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(binding.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, binding.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: previous.Revision},
	)
	change.Mutations = append(change.Mutations, binding.Mutations...)
	return change, nil
}
