package servicelifecycle

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *AgentWork) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	captured, _, err := owner.inputs.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	budget := 30 * time.Second
	if captured.Runtime != nil {
		budget = 120 * time.Second
		procedure, err := BuildProcedure(captured)
		if err != nil {
			return workqueue.Change{}, err
		}
		if procedure.Hook != nil {
			minimum := time.Duration(procedure.Hook.Definition.TimeoutSeconds)*time.Second + 30*time.Second
			if minimum > budget {
				budget = minimum
			}
		}
	}
	deadline := at.Add(budget)
	if item.Item.AttemptNumber == 0 {
		return owner.inputs.PrepareUnstartedRetry(ctx, view, at, deadline)
	}
	input, err := BuildWorkInput(captured)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.Item.Phase == workexecution.Attention {
		return owner.inputs.PrepareRecoveryRetry(ctx, view, previous, input, at, deadline)
	}
	return owner.inputs.PrepareForwardRetry(ctx, view, previous, input, at, deadline)
}
