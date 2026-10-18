package backingprovisionwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareQueuedStop closes only never-assigned work or a queued forward Retry
// whose effects were already settled. Cleanup permission is merely revoked:
// its retained claims cannot be released by pretending no helper ran.
func (repository *Repository) PrepareQueuedStop(ctx context.Context, view workqueue.View,
	outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	input, guard, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(input.Wire)
	if view.Item.Item.ActiveAttemptID != "" || view.Item.Item.Cleanup != workexecution.CleanupNone {
		change, err := workqueue.PrepareRevocation(view.Task, view.Item, outcome, at)
		change.Conditions = append(change.Conditions, guard)
		return change, err
	}
	result := workexecution.Completion{Outcome: outcome, Evidence: *view.Item.Item.Input, EffectsSettled: true}
	var settled workqueue.Change
	if view.Item.Item.AttemptNumber == 0 {
		settled, err = workqueue.PrepareUnstartedSettlement(view.Task, view.Item, result, at)
	} else {
		queue, queueErr := workqueue.NewRepository(repository.store)
		if queueErr != nil {
			return workqueue.Change{}, queueErr
		}
		_, previous, readErr := queue.ReadAttempt(ctx, view.Task.Task.ID, view.Item.Item.ID, view.Item.Item.AttemptNumber, view.ReadRevision)
		if readErr != nil {
			return workqueue.Change{}, readErr
		}
		if previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil {
			return workqueue.Change{}, invalid("queued backing creation Retry has no settled preceding attempt")
		}
		result.Evidence = *previous.Attempt.Result
		settled, err = workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, result, at)
		settled.Conditions = append(settled.Conditions, keyvalue.Condition{Key: workqueue.AttemptKey(previous.Attempt.ID), ModRevision: previous.Revision})
	}
	if err != nil {
		keyvalue.ClearMutationValues(settled.Mutations)
		return workqueue.Change{}, err
	}
	state, err := repository.prepareDirectoryState(ctx, view, input, false)
	if err != nil {
		keyvalue.ClearMutationValues(settled.Mutations)
		return workqueue.Change{}, err
	}
	settled.Conditions = append(settled.Conditions, guard)
	settled.Conditions = append(settled.Conditions, state.Conditions...)
	settled.Mutations = append(settled.Mutations, state.Mutations...)
	return settled, nil
}
