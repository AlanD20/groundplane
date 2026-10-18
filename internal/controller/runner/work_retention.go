package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (executor *Executor) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	return executor.inputs.PrepareRetention(ctx, view, at, final)
}

func (executor *Executor) PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return runnerwork.PrepareAttemptRetirement(view, snapshot)
}
