package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	if view.Item.Item.Operation == workexecution.Backup {
		return owner.inputs.PrepareCaptureRetention(ctx, view, at, final)
	}
	if view.Item.Item.Operation == workexecution.Restore {
		return owner.inputs.PrepareRestoreRetention(ctx, view, at, final)
	}
	return owner.inputs.PreparePruneRetention(ctx, view, at, final)
}

func (owner *AgentWork) PrepareAttemptRetirement(
	view workqueue.View,
	attempt workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	return backupwork.PrepareAttemptRetirement(view, attempt)
}
