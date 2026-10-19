package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *Executor) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	input, err := executor.inputs.Load(ctx, item.ID, 0)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if err := input.Record.MatchWork(item); err != nil {
		return workdispatch.Decision{}, err
	}
	if !at.Before(item.Deadline) {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			if err != nil {
				return workdispatch.Decision{}, err
			}
			change.Conditions = append(
				change.Conditions,
				keyvalue.Condition{Key: runnerwork.InputKey(item.ID), ModRevision: input.Revision},
			)
			return workdispatch.Decision{Disposition: workdispatch.Advance, Change: change}, nil
		}
		change, err := executor.settleUnstarted(ctx, request.Task, request.Item, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{
			Conditions: []keyvalue.Condition{{Key: runnerwork.InputKey(item.ID), ModRevision: input.Revision}},
		}}, nil
}

func (executor *Executor) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner Task has no stop request")
	}
	if item.Item.ActiveAttemptID == "" && item.Item.Cleanup == workexecution.CleanupNone &&
		(item.Item.Phase == workexecution.Ready || item.Item.Phase == workexecution.Waiting) {
		return executor.settleUnstarted(ctx, task, item, task.Task.Stop.Outcome, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (executor *Executor) settleUnstarted(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	input, err := executor.inputs.Load(ctx, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := input.Record.MatchWork(item.Item); err != nil {
		return workqueue.Change{}, err
	}
	var resource workqueue.Change
	if input.Record.Operation == runnerallocation.RuntimeOperationRemove {
		resource, err = executor.inputs.PrepareUnstartedRemoval(ctx, input, item.Item)
	} else {
		resource, err = executor.repository.PrepareUnstartedRunnerCreation(ctx, input, item.Item)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	result := workexecution.Completion{Outcome: outcome, Evidence: *item.Item.Input, EffectsSettled: true}
	var change workqueue.Change
	if item.Item.AttemptNumber == 0 {
		change, err = workqueue.PrepareUnstartedSettlement(task, item, result, at)
	} else {
		change, err = workqueue.PrepareQueuedRetrySettlement(task, item, result, at)
	}
	if err != nil {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Mutations = append(change.Mutations, resource.Mutations...)
	return change, nil
}
