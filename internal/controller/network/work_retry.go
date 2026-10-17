package network

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time) (workqueue.Change, error) {
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	if item.Item.AttemptNumber == 0 {
		return owner.inputs.PrepareUnstartedRetry(ctx, view, at)
	}
	if item.Item.Phase == workexecution.Attention {
		return owner.inputs.PrepareRecoveryRetry(ctx, view, previous, at)
	}
	return owner.inputs.PrepareForwardRetry(ctx, view, previous, at)
}
