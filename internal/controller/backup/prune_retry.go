package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	return owner.inputs.PrepareRecoveryRetry(
		ctx,
		workqueue.View{Task: task, Item: item, ReadRevision: revision},
		previous,
		at,
	)
}
