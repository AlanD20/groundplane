package volume

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish closes a direct resource action only after its work owner has
// atomically settled the host result and resource state.
func (owner *AgentWork) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.ExpectedItems != 1 || len(finished.Items) != 1 || (task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume action requires one settled resource operation")
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.Volume || item.Item.TaskID != task.ID ||
		item.Item.TargetID != task.TargetID || item.Item.Phase != workexecution.Settled ||
		item.Item.Result == nil || item.Item.ActiveAttemptID != "" ||
		(item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume action has unresolved resource work")
	}
	view := workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision}
	input, guards, err := owner.inputs.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.VolumeId != task.TargetID || input.EnvironmentId != task.Scope.EnvironmentID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume result belongs to another resource")
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision})
	return workqueue.Change{Conditions: guards}, nil
}
