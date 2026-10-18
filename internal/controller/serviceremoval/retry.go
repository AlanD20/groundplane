package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	captured, _, err := owner.inputs.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	// Retry gets a bounded execution window, not the elapsed age of the Task.
	deadline := at.Add(2 * time.Minute)
	if item.Item.AttemptNumber == 0 {
		return owner.inputs.PrepareUnstartedRetry(ctx, view, at, deadline)
	}
	input, err := BuildHostInput(captured)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.Item.Phase == workexecution.Attention {
		return owner.inputs.PrepareRecoveryRetry(ctx, view, previous, input, at, deadline)
	}
	if item.Item.Cleanup == workexecution.CleanupComplete {
		return owner.inputs.PrepareRestoredRetry(ctx, view, previous, input, at, deadline)
	}
	return owner.inputs.PrepareUnaffectedRetry(ctx, view, previous, input, at, deadline)
}
