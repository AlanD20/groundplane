package softwareactivation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	record, err := service.records.At(ctx, request.Task.Task.ID, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if err := record.Record.MatchStage(request.Item.Item); err != nil {
		return workdispatch.Decision{}, err
	}
	guard := activationGuard(record)
	if !at.Before(request.Item.Item.Deadline) {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			change.Conditions = append(change.Conditions, guard.Conditions...)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		reference, err := record.Record.Reference()
		if err != nil {
			return workdispatch.Decision{}, err
		}
		change, err := workqueue.PrepareUnstartedSettlement(
			request.Task,
			request.Item,
			workexecution.Completion{Outcome: workexecution.Failed, EffectsSettled: true, Evidence: reference},
			at,
		)
		change.Conditions = append(change.Conditions, guard.Conditions...)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch,
		Deadline:    request.Item.Item.Deadline,
		Change:      guard,
	}, nil
}

func (service *Service) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "software activation has no stop request")
	}
	if item.Item.AttemptNumber != 0 {
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	record, err := service.records.At(ctx, task.Task.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := record.Record.MatchStage(item.Item); err != nil {
		return workqueue.Change{}, err
	}
	reference, err := record.Record.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(
		task,
		item,
		workexecution.Completion{Outcome: task.Task.Stop.Outcome, EffectsSettled: true, Evidence: reference},
		at,
	)
	change.Conditions = append(change.Conditions, activationGuard(record).Conditions...)
	return change, err
}

func (service *Service) PrepareWorkRetry(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot,
	revision int64,
	at time.Time,
) (workqueue.Change, error) {
	if item.Item.Phase != workexecution.Attention || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Retry only cleans up interrupted staging; apply the preparation again to activate it",
		)
	}
	record, err := service.records.At(ctx, task.Task.ID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := record.Record.MatchStage(item.Item); err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(
		task,
		item,
		previous,
		workexecution.RetryDecision{
			Direction: workexecution.Recovery,
			Result:    *previous.Attempt.Result,
			NotBefore: at,
			Deadline:  at.Add(2 * time.Minute),
		},
		at,
	)
	change.Conditions = append(change.Conditions, activationGuard(record).Conditions...)
	return change, err
}
