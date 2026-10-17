package detachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish joins the public Task outcome to its settled native work.
// Resource deletion or failed-binding retention already joined item settlement;
// finishing diagnostic history must not execute or repeat those effects.
func (owner *Owner) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionDetach || len(finished.Items) != 1 || task.ExpectedItems != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach action has no single captured work result")
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.Detach || item.Item.TargetID != task.TargetID || item.Item.Phase != workexecution.Settled ||
		item.Item.Result == nil || item.Item.ActiveAttemptID != "" ||
		(item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach action still owns unresolved resource effects")
	}
	input, conditions, err := owner.inputs.LoadDetach(ctx, workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Clear()
	source := input.Snapshot.Source
	if source.Attach.ID != task.TargetID || task.Scope != (workexecution.Scope{TenantID: source.TenantID,
		ProjectID: source.ProjectID, EnvironmentID: source.Attach.EnvironmentID}) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach result differs from its captured scope")
	}
	conditions = append(conditions, keyvalue.Condition{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision})
	return workqueue.Change{Conditions: conditions}, nil
}
