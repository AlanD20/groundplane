package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish closes a direct Remove only after its operation owner has
// atomically settled resource state. It never infers success from a missing
// Service, which could instead belong to a later operation.
func (owner *AgentWork) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionRemove || task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal requires one settled operation")
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.ServiceRemoval || item.Item.TaskID != task.ID ||
		item.Item.TargetID != task.TargetID || item.Item.Phase != workexecution.Settled || item.Item.Result == nil ||
		item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Service removal still has unresolved execution or cleanup",
		)
	}
	_, capture, err := owner.inputs.Load(
		ctx,
		workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{capture,
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
	}}, nil
}
