package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish closes the visible action after work settlement has committed
// the runtime result and allocation changes together. It never repeats effects.
func (executor *Executor) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner action requires one settled work item")
	}
	item := finished.Items[0]
	if item.Item.Phase != workexecution.Settled || item.Item.Result == nil || item.Item.ActiveAttemptID != "" ||
		item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner action has unresolved runtime effects")
	}
	input, err := executor.inputs.Load(ctx, item.Item.ID, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := input.Record.MatchWork(item.Item); err != nil {
		return workqueue.Change{}, err
	}
	action := workexecution.ActionCreate
	if input.Record.Operation == runnerallocation.RuntimeOperationRemove {
		action = workexecution.ActionRemove
	}
	desired := input.Record.Desired
	if task.ID != input.Record.TaskID || task.Action != action || task.TargetID != desired.ID ||
		task.Scope != (workexecution.Scope{TenantID: desired.TenantID, ProjectID: desired.ProjectID(), EnvironmentID: desired.EnvironmentID()}) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner completion differs from its captured action")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
		{Key: runnerwork.InputKey(item.Item.ID), ModRevision: input.Revision},
	}}, nil
}
