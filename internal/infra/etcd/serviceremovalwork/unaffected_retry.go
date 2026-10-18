package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareUnaffectedRetry restores only the admission fence for a forward attempt
// that failed before any effect grant. Partial removal/restoration requires its
// separate recovery receipt, never this zero-effect route.
func (repository *Repository) PrepareUnaffectedRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, delivery *agentpb.ServiceLifecycleWorkInput, at, deadline time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.Stop != nil || task.CollectionStartedAt != nil || item.ForwardRevoked ||
		item.Operation != workexecution.ServiceRemoval || item.Phase != workexecution.Settled ||
		item.Outcome != workexecution.Failed || item.Cleanup != workexecution.CleanupNone ||
		item.Input == nil || item.Result == nil || attempt.Result == nil || *item.Result != *attempt.Result ||
		attempt.Direction != workexecution.Forward || attempt.Number != item.AttemptNumber ||
		attempt.Number == ^uint64(0) || attempt.FinishedAt == nil || !deadline.After(at) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Service removal Retry requires a failed action without host effects")
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
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Service removal already received an effect grant; recovery proof is required")
	}
	prior, priorGuard, err := repository.LoadContinuation(ctx, view, attempt, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if prior != nil {
		completed, err := prior.Completed(delivery)
		if err != nil {
			return workqueue.Change{}, err
		}
		if len(completed) != 0 {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Service removal retains earlier host effects; recovery proof is required")
		}
	}
	continuation := lifecycleexecution.Continuation{Source: lifecycleexecution.Recovery{
		Forward: attempt, Failure: failure, Evidence: *attempt.Result,
	}}
	encoded, err := lifecycleexecution.EncodeContinuation(delivery, continuation)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := repository.prepareRetryFence(ctx, view, input, at)
	if err != nil {
		clear(encoded)
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Forward, Result: *attempt.Result, NotBefore: at, Deadline: deadline,
	}, at)
	if err != nil {
		clear(encoded)
		keyvalue.ClearMutationValues(fence.Mutations)
		return workqueue.Change{}, err
	}
	continuationKey := servicelifecyclework.ContinuationKey(item.ID, attempt.Number+1)
	change.Conditions = append(change.Conditions, capture, failureGuard, priorGuard, keyvalue.Condition{Key: continuationKey})
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Mutations = append(change.Mutations, fence.Mutations...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: continuationKey, Value: encoded})
	return change, nil
}
