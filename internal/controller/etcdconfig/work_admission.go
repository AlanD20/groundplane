package etcdconfig

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (s *Service) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	input, err := s.inputs.Load(ctx, request.Item.Item.ID, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if err := input.Record.Match(request.Item.Item); err != nil {
		return workdispatch.Decision{}, err
	}
	guard := inputGuard(input)
	if !at.Before(request.Item.Item.Deadline) {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			change.Conditions = append(change.Conditions, guard.Conditions...)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		change, err := s.settleUnstarted(request.Task, request.Item, input, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch,
		Deadline:    request.Item.Item.Deadline,
		Change:      guard,
	}, nil
}

func (s *Service) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "etcd configuration has no stop request")
	}
	if item.Item.AttemptNumber != 0 {
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	input, err := s.inputs.Load(ctx, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	return s.settleUnstarted(task, item, input, task.Task.Stop.Outcome, at)
}

func (s *Service) settleUnstarted(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	input keyvalue.Versioned[etcdconfiguration.Input], outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	if err := input.Record.Match(item.Item); err != nil {
		return workqueue.Change{}, err
	}
	reference, err := input.Record.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(task, item,
		workexecution.Completion{Outcome: outcome, Evidence: reference, EffectsSettled: true}, at)
	change.Conditions = append(change.Conditions, inputGuard(input).Conditions...)
	return change, err
}

func inputGuard(input keyvalue.Versioned[etcdconfiguration.Input]) workqueue.Change {
	return workqueue.Change{
		Conditions: []keyvalue.Condition{
			{Key: etcdconfiguration.InputKey(input.Record.WorkItemID), ModRevision: input.Revision},
		},
	}
}

// Retry is cleanup-only. A failed or restored Apply requires a new operator
// Apply to select configuration again; it never replays forward activation.
func (s *Service) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if item.Item.Phase != workexecution.Attention || previous.Attempt.Result == nil || item.Item.Input == nil {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"etcd Apply is not retried; submit a new Apply after its effects are settled",
		)
	}
	input, err := s.inputs.Load(ctx, item.Item.ID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := input.Record.Match(item.Item); err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(etcdconfiguration.Timeout),
	}, at)
	change.Conditions = append(change.Conditions, inputGuard(input).Conditions...)
	return change, err
}
