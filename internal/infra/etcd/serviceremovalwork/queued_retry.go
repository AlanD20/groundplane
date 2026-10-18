package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareQueuedRetryStop releases the reacquired deletion fence only when the
// preceding failure proves there were no host effects. A queued recovery cannot
// use this path: its protected effects still require restoration proof.
func (repository *Repository) PrepareQueuedRetryStop(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, delivery *agentpb.ServiceLifecycleWorkInput, at time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if item.Operation != workexecution.ServiceRemoval || item.AttemptNumber == 0 || item.Input == nil ||
		item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone || !task.PublicationComplete ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || previous.Revision <= 0 ||
		attempt.Direction != workexecution.Forward || attempt.Number != item.AttemptNumber ||
		attempt.FinishedAt == nil || attempt.Result == nil || at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Service removal queued Retry has no settled forward failure")
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, conflict("Service removal queued Retry has not stopped or expired")
	}
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, failureGuard, err := repository.LoadFailure(ctx, workqueue.ResumeView{Work: view, Attempt: previous}, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	failure, err := lifecycleexecution.DecodeFailure(attempt, delivery, raw, *attempt.Result)
	if err != nil {
		return workqueue.Change{}, err
	}
	if failure.Checkpoint != nil {
		return workqueue.Change{}, conflict("Service removal received an effect grant and still needs recovery proof")
	}
	prior, continuationGuard, err := repository.LoadContinuation(ctx, view, attempt, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if prior != nil {
		completed, err := prior.Completed(delivery)
		if err != nil {
			return workqueue.Change{}, err
		}
		if len(completed) != 0 {
			return workqueue.Change{}, conflict("Service removal retains host effects from an earlier attempt")
		}
	}
	fence, err := repository.DeletionGuard(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *attempt.Result, EffectsSettled: true,
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture, failureGuard, continuationGuard, fence,
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: previous.Revision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: fence.Key})
	return change, nil
}
