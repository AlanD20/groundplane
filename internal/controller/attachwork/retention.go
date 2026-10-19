package attachwork

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"
)

func (owner *Reports) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	return owner.inputs.repository.PrepareRetention(ctx, view, at, final)
}

func (owner *Reports) PrepareAttemptRetirement(
	view workqueue.View,
	attempt workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	return attachprovision.PrepareAttemptRetirement(view, attempt)
}
