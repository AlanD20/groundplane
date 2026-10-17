package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DefinitionPreparer interface {
	PrepareDefinitionSettlement(context.Context, workqueue.View, blueprintunits.Unit, blueprintunits.Snapshot, time.Time) (workqueue.Change, error)
	PrepareDNSSettlement(context.Context, workqueue.View, blueprintunits.Unit, blueprintunits.Snapshot, time.Time) (etcd.BlueprintDNSPreparation, error)
}

type DefinitionWork struct {
	service DefinitionPreparer
	ledger  *blueprintunits.Repository
}

func NewDefinitionWork(service DefinitionPreparer, ledger *blueprintunits.Repository) (*DefinitionWork, error) {
	if service == nil || ledger == nil {
		return nil, errs.New(errs.KindInternal, "Definition work requires Blueprint service and ledger")
	}
	return &DefinitionWork{service: service, ledger: ledger}, nil
}

func (owner *DefinitionWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if item.Operation != workexecution.ResourceDefinition || item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Definition work is not an unstarted Controller decision")
	}
	if !at.Before(item.Deadline) {
		change, err := blueprintunits.PrepareDefinitionWorkStop(request.Task, request.Item, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	if !request.DecisionOnly || request.ReadRevision <= 0 {
		return workdispatch.Decision{}, errs.New(errs.KindInternal, "Definition work requires decision admission")
	}
	snapshot, err := owner.ledger.LoadAtRevision(ctx, request.Task.Task.Scope.EnvironmentID, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if snapshot.Desired == nil || snapshot.HeadTaskID != item.TaskID || snapshot.Desired.Record.ParentTaskID != item.TaskID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Definition work no longer owns the captured Apply")
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
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Definition work differs from its captured graph")
		}
		expected, err := blueprintunits.DefinitionWorkInput(request.Task.Task, unit)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		if item.Input == nil || *item.Input != expected || captured.Input == nil || *captured.Input != expected {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Definition work differs from captured input")
		}
		view := workqueue.View{
			Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision,
		}
		var change workqueue.Change
		disposition := workdispatch.Settle
		if unit.Target.Kind == ids.KindDNSRecord {
			prepared, prepareErr := owner.service.PrepareDNSSettlement(ctx, view, unit, snapshot, at)
			if prepareErr != nil {
				return workdispatch.Decision{}, prepareErr
			}
			change = prepared.Change
			if !prepared.Complete {
				if len(change.Mutations) == 0 {
					return workdispatch.Decision{Disposition: workdispatch.Wait, Wait: &workexecution.Wait{Reason: workexecution.WaitInput}}, nil
				}
				fence, fenceErr := workqueue.PrepareDecisionFence(request)
				if fenceErr != nil {
					keyvalue.ClearMutationValues(change.Mutations)
					return workdispatch.Decision{}, fenceErr
				}
				change.Conditions = append(change.Conditions, fence.Conditions...)
				disposition = workdispatch.Advance
			}
		} else {
			change, err = owner.service.PrepareDefinitionSettlement(ctx, view, unit, snapshot, at)
		}
		if err == nil {
			change.Conditions = append(change.Conditions, keyvalue.Condition{
				Key:         blueprintunits.CapturedGraphKey(graph.Record.EnvironmentID, graph.Record.TaskID),
				ModRevision: graph.Revision,
			})
		}
		return workdispatch.Decision{Disposition: disposition, Change: change}, err
	}
	return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Definition is absent from the captured Apply")
}

func (owner *DefinitionWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Definition work has no stop request")
	}
	return blueprintunits.PrepareDefinitionWorkStop(task, item, task.Task.Stop.Outcome, at)
}

func (owner *DefinitionWork) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	if !final {
		return false, workqueue.Change{}, errs.New(errs.KindInternal, "Definition decision unexpectedly has attempts")
	}
	change, err := owner.ledger.PrepareDefinitionWorkRetention(ctx, view, at)
	return err == nil, change, err
}

func (owner *DefinitionWork) PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindInternal, "Controller Definition decision cannot have execution attempts")
}
