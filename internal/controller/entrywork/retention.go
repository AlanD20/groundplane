package entrywork

import (
	"context"
	"time"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	return owner.inputs.PrepareRetention(ctx, view, at, final)
}

func (owner *AgentWork) PrepareAttemptRetirement(
	view workqueue.View,
	attempt workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	return entryrecord.PrepareAttemptRetirement(view, attempt)
}
