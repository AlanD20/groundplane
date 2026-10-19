package scripts

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RemovalWork struct {
	records *scriptremoval.Repository
	queue   *workqueue.Repository
}

func NewRemovalWork(records *scriptremoval.Repository, queue *workqueue.Repository) (*RemovalWork, error) {
	if records == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Script removal execution storage is required")
	}
	return &RemovalWork{records: records, queue: queue}, nil
}

func (owner *RemovalWork) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item, task := request.Item.Item, request.Task.Task
	if !request.DecisionOnly || request.Direction != workexecution.Forward || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"Script removal is an atomic Controller decision",
		)
	}
	outcome := workexecution.Succeeded
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		outcome = workexecution.Failed
	}
	change, err := owner.records.PrepareDecision(
		ctx,
		workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision},
		outcome,
		at,
	)
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
}

func (owner *RemovalWork) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script removal has no stop request")
	}
	view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script removal changed before cancellation")
	}
	return owner.records.PrepareDecision(ctx, view, task.Task.Stop.Outcome, at)
}

func (owner *RemovalWork) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	return owner.records.PrepareRetention(ctx, view, at, final)
}

func (owner *RemovalWork) PrepareAttemptRetirement(
	view workqueue.View,
	attempt workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script deletion cannot own host execution attempts")
}
