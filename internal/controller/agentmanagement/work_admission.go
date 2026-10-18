package agentmanagement

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	input, err := owner.inputs.Load(ctx, request.Item.Item, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if !at.Before(request.Item.Item.Deadline) {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			change.Conditions = append(change.Conditions, inputGuard(input).Conditions...)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		change, err := owner.settleUnstarted(request.Task, request.Item, input, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	deadline := request.Item.Item.Deadline
	limit := 120 * time.Second
	if input.Record.Action == workexecution.ActionUpdate {
		limit = 300 * time.Second
	}
	if request.Direction == workexecution.Forward && at.Add(limit).Before(deadline) {
		deadline = at.Add(limit)
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline, Change: inputGuard(input)}, nil
}

func (owner *Work) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Agent lifecycle has no stop request")
	}
	if item.Item.AttemptNumber == 0 {
		input, err := owner.inputs.Load(ctx, item.Item, 0)
		if err != nil {
			return workqueue.Change{}, err
		}
		return owner.settleUnstarted(task, item, input, task.Task.Stop.Outcome, at)
	}
	owner.mu.Lock()
	if active := owner.active[task.Task.ID]; active != nil && active.attemptID == item.Item.ActiveAttemptID {
		active.stopped = true
		active.cancel()
	}
	owner.mu.Unlock()
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *Work) settleUnstarted(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, input keyvalue.Versioned[agentoperations.Capture], outcome workexecution.Outcome, at time.Time) (workqueue.Change, error) {
	reference, err := input.Record.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(task, item, workexecution.Completion{Outcome: outcome, Evidence: reference, EffectsSettled: true}, at)
	change.Conditions = append(change.Conditions, inputGuard(input).Conditions...)
	return change, err
}

func (owner *Work) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time) (workqueue.Change, error) {
	if item.Item.Phase != workexecution.Attention || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Agent lifecycle Retry only reconciles retained effects; a new action is required after settlement")
	}
	input, err := owner.inputs.Load(ctx, item.Item, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{Direction: workexecution.Recovery,
		Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(localagent.RemovalRecoveryTimeout)}, at)
	change.Conditions = append(change.Conditions, inputGuard(input).Conditions...)
	return change, err
}
