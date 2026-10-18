package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EntryPreparer interface {
	PrepareEntrySettlement(context.Context, workqueue.View, blueprintunits.Unit, blueprintunits.Snapshot) (workqueue.Change, error)
}

type EntryWork struct {
	service EntryPreparer
	ledger  *blueprintunits.Repository
}

func NewEntryWork(service EntryPreparer, ledger *blueprintunits.Repository) (*EntryWork, error) {
	if service == nil || ledger == nil {
		return nil, errs.New(errs.KindInternal, "Entry work requires Blueprint service and ledger")
	}
	return &EntryWork{service: service, ledger: ledger}, nil
}

func (owner *EntryWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if item.Operation != workexecution.EntrySettlement || item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Entry work is not an unstarted Controller decision")
	}
	if !at.Before(item.Deadline) {
		change, err := blueprintunits.PrepareEntryWorkStop(request.Task, request.Item, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	if !request.DecisionOnly || request.ReadRevision <= 0 {
		return workdispatch.Decision{}, errs.New(errs.KindInternal, "Entry work requires decision admission")
	}
	snapshot, err := owner.ledger.LoadAtRevision(ctx, request.Task.Task.Scope.EnvironmentID, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if snapshot.Desired == nil || snapshot.HeadTaskID != item.TaskID || snapshot.Desired.Record.ParentTaskID != item.TaskID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Entry work no longer owns the captured Apply")
	}
	graph, err := owner.ledger.LoadCapturedGraph(ctx, request.Task.Task, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		unit := graph.Record.Units[index]
		if captured.Operation != item.Operation || captured.TargetID != item.TargetID || captured.Placement != item.Placement {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Entry work differs from its captured graph")
		}
		expected, err := blueprintunits.EntryWorkInput(request.Task.Task, unit)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		if item.Input == nil || *item.Input != expected || captured.Input == nil || *captured.Input != expected {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Entry work differs from captured input")
		}
		change, err := owner.service.PrepareEntrySettlement(ctx, workqueue.View{
			Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision,
		}, unit, snapshot)
		if err == nil {
			change.Conditions = append(change.Conditions, keyvalue.Condition{
				Key:         blueprintunits.CapturedGraphKey(graph.Record.EnvironmentID, graph.Record.TaskID),
				ModRevision: graph.Revision,
			})
		}
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Entry is absent from the captured Apply")
}

func (owner *EntryWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Entry work has no stop request")
	}
	return blueprintunits.PrepareEntryWorkStop(task, item, task.Task.Stop.Outcome, at)
}

func (owner *EntryWork) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	if !final {
		return false, workqueue.Change{}, errs.New(errs.KindInternal, "Entry decision unexpectedly has attempts")
	}
	change, err := owner.ledger.PrepareEntryWorkRetention(ctx, view, at)
	return err == nil, change, err
}

func (owner *EntryWork) PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindInternal, "Controller Entry decision cannot have execution attempts")
}
