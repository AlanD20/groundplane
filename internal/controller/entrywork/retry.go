package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.EntryConfiguration || item.Item.Placement.Executor != workexecution.Agent || item.Item.AttemptNumber == 0 {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Entry Retry requires a captured execution attempt")
	}
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	if item.Item.Phase == workexecution.Attention {
		return owner.inputs.PrepareRecoveryRetry(ctx, view, previous, at)
	}
	return owner.inputs.PrepareForwardRetry(ctx, view, previous, at)
}
