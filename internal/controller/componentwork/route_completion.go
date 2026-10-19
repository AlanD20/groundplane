package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	componentrecords "github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeCompletion struct {
	inputs *componentrecords.InputRepository
}

// RouteCompletions owns the complete set of direct Route actions. Blueprint
// Component actions retain their separate aggregate completion owner.
func RouteCompletions(inputs *componentrecords.InputRepository) ([]workdispatch.CompletionRegistration, error) {
	if inputs == nil {
		return nil, errs.New(errs.KindInternal, "Route completion requires captured Component storage")
	}
	owner := &routeCompletion{inputs: inputs}
	return []workdispatch.CompletionRegistration{
		{Action: workexecution.ActionCreate, Operation: workexecution.Component, Owner: owner},
		{Action: workexecution.ActionUpdate, Operation: workexecution.Component, Owner: owner},
		{Action: workexecution.ActionRemove, Operation: workexecution.Component, Owner: owner},
	}, nil
}

func (owner *routeCompletion) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.ExpectedItems != 1 || len(finished.Items) != 1 || ids.Validate(ids.KindRoute, task.TargetID) != nil ||
		(task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Route completion requires one settled Route action",
		)
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.Component || item.Item.TaskID != task.ID ||
		item.Item.Phase != workexecution.Settled ||
		item.Item.ActiveAttemptID != "" ||
		item.Item.Result == nil ||
		(item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Route action has unresolved work")
	}
	view := workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision}
	if item.Item.Placement.Executor == workexecution.Controller {
		conditions, err := owner.inputs.RouteDefinitionConditions(ctx, view)
		return workqueue.Change{Conditions: conditions}, err
	}
	_, capture, conditions, err := owner.inputs.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if capture.Route == nil || capture.Route.Record.Desired.ID != task.TargetID ||
		capture.Route.Record.EnvironmentID != task.Scope.EnvironmentID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Route result belongs to another resource")
	}
	conditions = append(
		conditions,
		keyvalue.Condition{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
	)
	return workqueue.Change{Conditions: conditions}, nil
}
