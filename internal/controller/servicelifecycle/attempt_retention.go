package servicelifecycle

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"
)

func (owner *AgentWork) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	return owner.inputs.PrepareRetention(ctx, view, at, final)
}

func (owner *AgentWork) PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return servicelifecyclework.PrepareAttemptRetirement(view, snapshot)
}
