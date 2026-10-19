package volume

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) PrepareWorkRetry(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot,
	revision int64,
	at time.Time,
) (workqueue.Change, error) {
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	var change workqueue.Change
	var err error
	if item.Item.AttemptNumber == 0 {
		change, err = owner.inputs.PrepareUnstartedRetry(ctx, view, at)
	} else if item.Item.Phase == workexecution.Attention {
		change, err = owner.inputs.PrepareRecoveryRetry(ctx, view, previous, at)
	} else {
		change, err = owner.inputs.PrepareForwardRetry(ctx, view, previous, at)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	ownership, err := owner.inputs.PrepareRetryOwnership(ctx, view)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, ownership.Conditions...)
	change.Mutations = append(change.Mutations, ownership.Mutations...)
	return change, nil
}
