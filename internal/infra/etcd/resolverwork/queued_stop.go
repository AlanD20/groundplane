package resolverwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *Repository) PrepareQueuedStop(ctx context.Context, view workqueue.View, outcome workexecution.Outcome, at time.Time) (workqueue.Change, error) {
	stored, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Item.Item.ActiveAttemptID != "" || view.Item.Item.Cleanup != workexecution.CleanupNone {
		change, err := workqueue.PrepareRevocation(view.Task, view.Item, outcome, at)
		change.Conditions = append(change.Conditions, stored.Guards...)
		return change, err
	}
	result := workexecution.Completion{Outcome: outcome, Evidence: stored.Reference, EffectsSettled: true}
	var change workqueue.Change
	if view.Item.Item.AttemptNumber == 0 {
		change, err = workqueue.PrepareUnstartedSettlement(view.Task, view.Item, result, at)
	} else {
		queue, err := workqueue.NewRepository(repository.store)
		if err != nil {
			return workqueue.Change{}, err
		}
		_, previous, err := queue.ReadAttempt(ctx, view.Task.Task.ID, view.Item.Item.ID, view.Item.Item.AttemptNumber, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil {
			return workqueue.Change{}, invalid("queued DNS resolver Retry has no preceding settled result")
		}
		result.Evidence = *previous.Attempt.Result
		change, err = workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, result, at)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptKey(previous.Attempt.ID), ModRevision: previous.Revision})
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, stored.Guards...)
	request, err := repository.prepareRequestResult(ctx, view, outcome)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, request.Conditions...)
	change.Mutations = append(change.Mutations, request.Mutations...)
	return change, nil
}
