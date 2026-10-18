package servicelifecyclework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareUnstarted settles only operations that never obtained an Agent attempt.
// Success is possible only when admission captured the absence of runtime.
func (repository *Repository) PrepareUnstarted(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone ||
		item.Input == nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Service lifecycle cannot settle as never assigned")
	}
	captured, guard, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	outcome := workexecution.Succeeded
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		outcome = workexecution.Failed
	} else if captured.Runtime != nil {
		return workqueue.Change{}, conflict("Service lifecycle requires its captured host execution")
	}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *item.Input, EffectsSettled: true,
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard)
	if outcome == workexecution.Succeeded {
		conditions, mutations, err := services.PrepareLifecycleRuntime(ctx, repository.store, captured.ServiceID,
			captured.Scope.EnvironmentID, captured.TaskID, captured.Action, view.ReadRevision, true)
		if err != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, conditions...)
		change.Mutations = append(change.Mutations, mutations...)
	}
	return change, nil
}
