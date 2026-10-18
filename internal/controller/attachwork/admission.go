package attachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Reports) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if request.Item.Item.Placement.Executor == workexecution.Controller {
		return owner.admitController(ctx, request, at)
	}
	if request.Direction == workexecution.Recovery {
		return owner.admitRecovery(ctx, request, at)
	}
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || request.Direction != workexecution.Forward || item.Operation != workexecution.Attach ||
		item.Placement.Executor != workexecution.Agent || item.Phase != workexecution.Ready || item.ActiveAttemptID != "" ||
		at.IsZero() || at.Before(item.UpdatedAt) ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Attach provisioning cannot obtain this host assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		change, err := owner.stopPrepared(ctx, view, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	current, guards, err := owner.inputs.repository.ReadDesiredMatch(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if !current {
		change, err := owner.revokeUnstarted(ctx, view, workexecution.Superseded, at)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		change.Conditions = append(change.Conditions, guards...)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, nil
	}
	bindings, err := owner.inputs.repository.PendingBindingConditions(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	deadline := item.Deadline
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline,
		Change: workqueue.Change{Conditions: append(guards, bindings...)}}, nil
}

func (owner *Reports) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil || item.Item.Operation != workexecution.Attach || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach provisioning has no matching stop request")
	}
	if item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone || item.Item.Input == nil {
		// Revocation is not proof that a previously authorized command stopped.
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach ownership changed while stopping queued work")
	}
	return owner.stopPrepared(ctx, view, at)
}

func (owner *Reports) revokeUnstarted(ctx context.Context, view workqueue.View, outcome workexecution.Outcome, at time.Time) (workqueue.Change, error) {
	if view.Item.Item.ActiveAttemptID != "" || view.Item.Item.Cleanup != workexecution.CleanupNone {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach revocation cannot discard an assigned execution")
	}
	binding, err := owner.inputs.repository.PrepareFailedBinding(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRevocation(view.Task, view.Item, outcome, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(binding.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, binding.Conditions...)
	change.Mutations = append(change.Mutations, binding.Mutations...)
	return change, nil
}
