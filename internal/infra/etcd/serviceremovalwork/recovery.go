package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RecoverySnapshot struct {
	Capture  removalexecution.RecoveryCapture
	Revision int64
}

func (repository *Repository) LoadRecovery(ctx context.Context, view workqueue.View, delivery *agentpb.ServiceLifecycleWorkInput) (RecoverySnapshot, error) {
	input, _, err := repository.Load(ctx, view)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if !matchesDelivery(input, delivery) {
		return RecoverySnapshot{}, conflict("Service removal recovery input differs from its capture")
	}
	read, err := repository.read(ctx, []string{RecoveryKey(input.WorkItemID)}, view.ReadRevision)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return RecoverySnapshot{}, conflict("Service removal recovery has no captured failed boundary")
	}
	capture, err := DecodeRecoveryCapture(delivery, read.Values[0].Value)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	forward := capture.Source.Forward
	if forward.TaskID != input.TaskID || forward.WorkItemID != input.WorkItemID ||
		forward.Executor.HostID != input.HostID || forward.Number > view.Item.Item.AttemptNumber ||
		view.Item.Item.Result == nil || capture.Source.Evidence != *view.Item.Item.Result {
		return RecoverySnapshot{}, conflict("Service removal recovery belongs to a different failed execution")
	}
	return RecoverySnapshot{Capture: capture, Revision: read.Values[0].ModRevision}, nil
}

// PrepareRecoveryRetry requeues the same protected failed boundary. Task Abort
// or supersession does not waive cleanup; a fresh desired action is never read.
func (repository *Repository) PrepareRecoveryRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, delivery *agentpb.ServiceLifecycleWorkInput, at, deadline time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.CollectionStartedAt != nil || item.Operation != workexecution.ServiceRemoval ||
		item.Phase != workexecution.Attention || item.ActiveAttemptID != "" || item.Input == nil ||
		attempt.FinishedAt == nil || attempt.Result == nil || attempt.Number != item.AttemptNumber ||
		attempt.Input != *item.Input || attempt.TaskID != task.ID || attempt.WorkItemID != item.ID ||
		attempt.Executor.HostID != item.Placement.HostID || !deadline.After(at) ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped {
		return workqueue.Change{}, conflict("Service removal has no unresolved captured cleanup to retry")
	}
	input, guard, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	saved, err := repository.LoadRecovery(ctx, view, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	switch attempt.Direction {
	case workexecution.Forward:
		if saved.Capture.Source.Forward.ID != attempt.ID || saved.Capture.Source.Evidence != *attempt.Result {
			return workqueue.Change{}, conflict("Service removal recovery source differs from the failed attempt")
		}
	case workexecution.Recovery:
		if item.CleanupResult == nil || *item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, conflict("Service removal recovery Retry has no matching cleanup result")
		}
		if err := saved.Capture.ValidateFor(attempt, delivery); err != nil {
			return workqueue.Change{}, err
		}
	default:
		return workqueue.Change{}, conflict("Service removal recovery direction is invalid")
	}
	fence, err := repository.DeletionGuard(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *attempt.Result, NotBefore: at, Deadline: deadline,
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard, fence,
		keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: saved.Revision})
	return change, nil
}
