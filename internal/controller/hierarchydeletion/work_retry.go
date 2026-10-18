package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	if item.Item.AttemptNumber == 0 {
		return owner.records.PrepareHierarchyUnstartedRetry(ctx, view, at)
	}
	if item.Item.Phase == workexecution.Attention {
		return owner.records.PrepareHierarchyRecoveryRetry(ctx, view, previous, at)
	}
	authority, budget, err := owner.records.PrepareHierarchyForwardRetry(ctx, view, previous, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Forward, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(budget),
	}, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(authority.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, authority.Conditions...)
	change.Mutations = append(change.Mutations, authority.Mutations...)
	return change, nil
}
