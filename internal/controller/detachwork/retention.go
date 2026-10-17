package detachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *Owner) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	return owner.inputs.PrepareDetachRetention(ctx, view, at, final)
}

func (owner *Owner) PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return attachremoval.PrepareDetachAttemptRetirement(view, snapshot)
}
