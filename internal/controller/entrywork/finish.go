package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish closes the public Entry action only after configuration or
// definition-only removal settled. It cannot grant or repeat host effects.
func (owner *AgentWork) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.ExpectedItems != 1 || len(finished.Items) != 1 || ids.Validate(ids.KindEnvEntry, task.TargetID) != nil ||
		(task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Entry completion requires one settled Entry action",
		)
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.EntryConfiguration || item.Item.TaskID != task.ID || item.Item.TargetID != task.Scope.EnvironmentID ||
		item.Item.Phase != workexecution.Settled || item.Item.ActiveAttemptID != "" ||
		item.Item.Result == nil ||
		(item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Entry action has unresolved work")
	}
	view := workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision}
	if item.Item.Placement.Executor == workexecution.Controller {
		conditions, err := owner.inputs.DefinitionConditions(ctx, view)
		return workqueue.Change{Conditions: conditions}, err
	}
	_, _, conditions, err := owner.inputs.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(
		conditions,
		keyvalue.Condition{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
	)
	return workqueue.Change{Conditions: conditions}, nil
}
