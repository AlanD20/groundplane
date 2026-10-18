package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if request.Direction == workexecution.Recovery {
		return owner.admitRecovery(ctx, request, at)
	}
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || request.Direction != workexecution.Forward || item.Operation != workexecution.Component ||
		item.Placement.Executor != workexecution.Agent || item.Phase != workexecution.Ready || item.ActiveAttemptID != "" ||
		at.IsZero() || at.Before(item.UpdatedAt) || request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component work cannot obtain this host assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		if item.AttemptNumber != 0 {
			change, err := owner.inputs.PrepareQueuedRetryStop(ctx, view, at)
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
		}
		change, err := owner.inputs.PrepareUnstarted(ctx, view, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	input, _, guards, err := owner.inputs.Load(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if input.ComponentId != item.TargetID || input.EnvironmentId != task.Scope.EnvironmentID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Component assignment differs from its captured resource")
	}
	current, desired, err := owner.inputs.ReadDesiredMatch(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	guards = append(guards, desired...)
	if !current {
		change, err := workqueue.PrepareRevocation(request.Task, request.Item, workexecution.Superseded, at)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		change.Conditions = append(change.Conditions, guards...)
		return workdispatch.Decision{Disposition: workdispatch.Advance, Change: change}, nil
	}
	resources, err := owner.inputs.AdmissionConditions(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	guards = append(guards, resources...)
	deadline := item.Deadline
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline,
		Change: workqueue.Change{Conditions: guards}}, nil
}

func (owner *AgentWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil || item.Item.Operation != workexecution.Component || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Component work has no matching stop request")
	}
	// Revocation does not release an active slot or claim, nor undo a host
	// mutation. The execution owner must separately account for those effects.
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}
