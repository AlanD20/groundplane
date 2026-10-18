package servicelifecycle

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DecisionWork handles only captured no-runtime operations. Host lifecycle work
// has a different placement and cannot enter this Controller-only owner.
type DecisionWork struct {
	records *servicelifecyclework.Repository
}

func NewDecisionWork(records *servicelifecyclework.Repository) (*DecisionWork, error) {
	if records == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle decisions require captured input storage")
	}
	return &DecisionWork{records: records}, nil
}

func (owner *DecisionWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil || item.Item.Operation != workexecution.ServiceLifecycle || item.Item.TaskID != task.Task.ID ||
		item.Item.Placement.Executor != workexecution.Controller || item.Item.AttemptNumber != 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service lifecycle decision has no matching stop request")
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *DecisionWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if !request.DecisionOnly || request.Direction != workexecution.Forward ||
		request.Item.Item.Placement.Executor != workexecution.Controller {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Service lifecycle decision cannot authorize host execution")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	captured, _, err := owner.records.Load(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if captured.Runtime != nil {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Service lifecycle decision has captured host effects")
	}
	change, err := owner.records.PrepareUnstarted(ctx, view, at)
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
}
