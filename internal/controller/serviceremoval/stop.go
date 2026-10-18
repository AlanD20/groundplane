package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	return prepareStop(ctx, owner.queue, owner.inputs, task, item, at)
}

func prepareStop(ctx context.Context, queue *workqueue.Repository, inputs *serviceremovalwork.Repository,
	task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time,
) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil || item.Item.Operation != workexecution.ServiceRemoval || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal has no matching stop request")
	}
	if item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone {
		// An assigned executor may still have effects. Revocation closes its
		// forward gate but cannot release the captured deletion fence.
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	view, err := queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal changed during Stop")
	}
	if item.Item.Input == nil {
		return workqueue.PrepareInputSettlement(view, at)
	}
	if item.Item.AttemptNumber == 0 {
		return inputs.PrepareUnstartedStop(ctx, view, at)
	}
	captured, _, err := inputs.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	delivery, err := BuildHostInput(captured)
	if err != nil {
		return workqueue.Change{}, err
	}
	_, previous, err := queue.ReadAttempt(ctx, task.Task.ID, item.Item.ID, item.Item.AttemptNumber, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	return inputs.PrepareQueuedRetryStop(ctx, view, previous, delivery, at)
}
