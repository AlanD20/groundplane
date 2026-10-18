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

type ComponentDefinitionWork struct {
	service *Service
	ledger  *blueprintunits.Repository
	queue   *workqueue.Repository
}

func NewComponentDefinitionWork(service *Service, ledger *blueprintunits.Repository, queue *workqueue.Repository) (*ComponentDefinitionWork, error) {
	if service == nil || ledger == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Component definition work requires Blueprint and execution storage")
	}
	return &ComponentDefinitionWork{service: service, ledger: ledger, queue: queue}, nil
}

func (owner *ComponentDefinitionWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	task, item := request.Task.Task, request.Item.Item
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if !request.DecisionOnly || item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component metadata requires unstarted Controller decision work")
	}
	if !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		change, err := blueprintunits.PrepareComponentDefinitionStop(view, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	snapshot, err := owner.ledger.LoadAtRevision(ctx, task.Scope.EnvironmentID, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	graph, err := owner.ledger.LoadCapturedGraph(ctx, task, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	var unit *blueprintunits.Unit
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		if captured.Operation != item.Operation || captured.TargetID != item.TargetID || captured.Placement != item.Placement {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component work differs from its captured graph")
		}
		unit = &graph.Record.Units[index]
		expected, err := blueprintunits.ComponentDefinitionInput(task, *unit)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		if item.Input == nil || captured.Input == nil || *item.Input != expected || *captured.Input != expected {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component metadata input differs from accepted intent")
		}
		break
	}
	if unit == nil {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component is absent from the captured Apply")
	}
	// Proxy preparation selects the required subset. These are actual queue
	// records at the same revision, never synthesized private child identities.
	var services []workqueue.View
	writes := make(map[string]bool, len(unit.Writes))
	for _, target := range unit.Writes {
		writes[target.ID] = true
	}
	for _, captured := range graph.Record.Items {
		if captured.Operation != workexecution.Release || !writes[captured.TargetID] {
			continue
		}
		work, err := owner.queue.ReadWork(ctx, task.ID, captured.ID, request.ReadRevision)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		if work.Item.Item.TargetID != captured.TargetID || work.Item.Item.Placement != captured.Placement {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Proxy Service work differs from the captured graph")
		}
		services = append(services, work)
	}
	prepared, err := owner.service.prepareDisabledComponentSelection(ctx, view, services, *unit, snapshot)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if prepared == nil {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component no longer has the captured disabled-runtime proof")
	}
	defer keyvalue.ClearMutationValues(prepared.Sources.Mutations)
	change, err := owner.service.repository.PrepareBlueprintDisabledComponent(ctx, view, *unit,
		prepared.Selection, prepared.Proxy, prepared.Sources, at)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{
		Key: blueprintunits.CapturedGraphKey(graph.Record.EnvironmentID, graph.Record.TaskID), ModRevision: graph.Revision,
	})
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, nil
}

func (owner *ComponentDefinitionWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision || task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Component stop snapshot changed")
	}
	done, err := owner.service.blueprintReleases.DiscardProxyPreparation(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !done {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Component stop is releasing unpublished proxy inputs")
	}
	return blueprintunits.PrepareComponentDefinitionStop(view, task.Task.Stop.Outcome, at)
}

func (owner *ComponentDefinitionWork) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	if !final {
		return false, workqueue.Change{}, errs.New(errs.KindInternal, "Component metadata cannot have attempts")
	}
	change, err := owner.ledger.PrepareComponentDefinitionRetention(ctx, view, at)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	done, err := owner.service.blueprintReleases.DiscardProxyPreparation(ctx, view)
	if err != nil || !done {
		return false, workqueue.Change{}, err
	}
	return true, change, nil
}

func (owner *ComponentDefinitionWork) PrepareAttemptRetirement(workqueue.View, workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindInternal, "Component metadata cannot have attempts")
}
