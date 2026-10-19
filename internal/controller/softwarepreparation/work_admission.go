package softwarepreparation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	snapshot, err := service.records.LoadWork(ctx, request.Item.Item, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if !at.Before(request.Item.Item.Deadline) {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			change.Conditions = append(change.Conditions, snapshot.Guard().Conditions...)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		change, err := settleUnstarted(request.Task, request.Item, snapshot, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch,
		Deadline:    request.Item.Item.Deadline,
		Change:      snapshot.Guard(),
	}, nil
}

func (service *Service) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "software preparation has no stop request")
	}
	if item.Item.AttemptNumber != 0 {
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	snapshot, err := service.records.LoadWork(ctx, item.Item, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	return settleUnstarted(task, item, snapshot, task.Task.Stop.Outcome, at)
}

func settleUnstarted(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, snapshot preparationrecord.WorkSnapshot,
	outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	reference, err := snapshot.Capture.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(
		task,
		item,
		workexecution.Completion{Outcome: outcome, EffectsSettled: true, Evidence: reference},
		at,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	operation, err := preparationrecord.PrepareUnstarted(snapshot, outcome, at)
	change.Conditions = append(change.Conditions, operation.Conditions...)
	change.Mutations = append(change.Mutations, operation.Mutations...)
	return change, err
}

func (service *Service) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if item.Item.Phase != workexecution.Attention || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"submit a new software preparation; Retry is available only for captured build cleanup",
		)
	}
	snapshot, err := service.records.LoadWork(ctx, item.Item, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(2 * time.Minute),
	}, at)
	change.Conditions = append(change.Conditions, snapshot.Guard().Conditions...)
	return change, err
}
