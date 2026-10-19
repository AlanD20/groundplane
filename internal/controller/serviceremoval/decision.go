package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DecisionRecords interface {
	PrepareServiceRemovalDecision(context.Context, workqueue.View, time.Time) (workqueue.Change, error)
}

type DecisionWork struct {
	records DecisionRecords
	inputs  *serviceremovalwork.Repository
	queue   *workqueue.Repository
}

func NewDecisionWork(
	records DecisionRecords,
	inputs *serviceremovalwork.Repository,
	queue *workqueue.Repository,
) (*DecisionWork, error) {
	if records == nil || inputs == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Service removal decisions require resource settlement storage")
	}
	return &DecisionWork{records: records, inputs: inputs, queue: queue}, nil
}

func (owner *DecisionWork) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	if !request.DecisionOnly || request.Direction != workexecution.Forward ||
		request.Item.Item.Placement.Executor != workexecution.Controller {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"Service removal decision cannot authorize host execution",
		)
	}
	change, err := owner.records.PrepareServiceRemovalDecision(ctx,
		workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}, at)
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
}

func (owner *DecisionWork) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil || item.Item.Operation != workexecution.ServiceRemoval ||
		item.Item.TaskID != task.Task.ID ||
		item.Item.Placement.Executor != workexecution.Controller ||
		item.Item.AttemptNumber != 0 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Service removal decision has no matching stop request",
		)
	}
	return prepareStop(ctx, owner.queue, owner.inputs, task, item, at)
}
