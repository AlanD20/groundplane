package servicelifecyclework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareQueuedRetryStop preserves the preceding execution result. It settles
// only the queued retry; it never declares an interrupted host effect resolved.
func (repository *Repository) PrepareQueuedRetryStop(ctx context.Context, view workqueue.View, previous workqueue.AttemptSnapshot, at time.Time) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if item.Operation != workexecution.ServiceLifecycle || item.AttemptNumber == 0 || item.Input == nil ||
		item.ActiveAttemptID != "" || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) || previous.Revision <= 0 ||
		attempt.TaskID != task.ID || attempt.WorkItemID != item.ID || attempt.Number != item.AttemptNumber ||
		attempt.FinishedAt == nil || attempt.Result == nil || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, conflict("Service lifecycle queued Retry lacks its preceding captured result")
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, conflict("Service lifecycle queued Retry has not stopped or expired")
	}
	_, guard, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *attempt.Result, EffectsSettled: true,
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard,
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: previous.Revision})
	return change, nil
}
