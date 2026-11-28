package publicingressoperator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (work *Work) PrepareFinish(
	_ context.Context, finished workqueue.FinishedWork, _ time.Time,
) (workqueue.Change, error) {
	if finished.Task.Task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, conflict("public ingress Task has an unexpected work graph")
	}
	item := finished.Items[0]
	if !matchesHost(finished.Task.Task, item.Item) || item.Item.Phase != workexecution.Settled ||
		item.Item.Result == nil || item.Item.ActiveAttemptID != "" {
		return workqueue.Change{}, conflict("public ingress Task has unresolved work")
	}
	// The operation's durable publication or restoration was committed with
	// the terminal attempt. An unstarted failure cleared its pending owner
	// in the same transaction as settlement.
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
	}}, nil
}

func (work *Work) PrepareWorkRetry(
	ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, _ int64, at time.Time,
) (workqueue.Change, error) {
	if !matchesHost(task.Task, item.Item) || task.Task.CollectionStartedAt != nil ||
		item.Item.Phase != workexecution.Attention || previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil ||
		previous.Attempt.Number != item.Item.AttemptNumber ||
		previous.Attempt.TaskID != task.Task.ID || previous.Attempt.WorkItemID != item.Item.ID {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"public ingress can retry only its exact pending recovery",
		)
	}
	state, _, err := work.captured(ctx, task.Task, item.Item)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !state.Pending.Authorized {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"public ingress recovery has no prior authorized effect",
		)
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at,
		Deadline: at.Add(activationTimeout),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, stateGuard(state).Conditions...)
	return change, nil
}

func (work *Work) RetireWork(
	ctx context.Context, view workqueue.View, _ time.Time, _ bool,
) (bool, workqueue.Change, error) {
	if !matchesHost(view.Task.Task, view.Item.Item) || view.Task.Task.CollectionStartedAt == nil {
		return false, workqueue.Change{}, conflict("public ingress work has no collection fence")
	}
	state, err := work.operator.ingress.Read(ctx, view.Item.Item.TargetID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if state.Pending != nil && state.Pending.TaskID == view.Task.Task.ID {
		return false, workqueue.Change{}, nil
	}
	return true, stateGuard(state), nil
}

func (work *Work) PrepareAttemptRetirement(
	view workqueue.View, attempt workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	if !matchesHost(view.Task.Task, view.Item.Item) || view.Task.Task.CollectionStartedAt == nil ||
		attempt.Attempt.TaskID != view.Task.Task.ID || attempt.Attempt.WorkItemID != view.Item.Item.ID ||
		attempt.Attempt.FinishedAt == nil {
		return workqueue.Change{}, conflict("public ingress attempt has no collection fence")
	}
	// The replay proof is sealed in the attempt reference; there is no
	// independent attempt payload to collect.
	return workqueue.Change{}, nil
}
