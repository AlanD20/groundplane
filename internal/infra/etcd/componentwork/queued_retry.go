package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareQueuedRetryStop settles an unassigned forward retry, not its preceding
// execution. Admission of that retry already required settled external effects;
// stopping the queue must neither run cleanup again nor discard the saved result.
func (repository *InputRepository) PrepareQueuedRetryStop(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Operation != workexecution.Component || item.AttemptNumber == 0 || item.Input == nil ||
		item.ActiveAttemptID != "" || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Component queued Retry has no settled execution owner")
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, conflict("Component queued Retry has not stopped or expired")
	}
	_, previous, err := repository.transactions.ReadAttempt(
		ctx,
		task.ID,
		item.ID,
		item.AttemptNumber,
		view.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	attempt := previous.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, conflict("Component queued Retry lacks the preceding captured result")
	}
	_, _, guards, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *attempt.Result, EffectsSettled: true,
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: previous.Revision},
	)
	return change, nil
}
