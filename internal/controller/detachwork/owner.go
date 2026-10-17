// Package detachwork owns native Detach execution. Captured adapter/hook input,
// consumer effects and metadata settlement stay under the original visible Task.
package detachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Owner struct {
	queue   *workqueue.Repository
	inputs  *attachremoval.InputRepository
	planner *attachplan.RemovalPlanner
}

func New(queue *workqueue.Repository, inputs *attachremoval.InputRepository, planner *attachplan.RemovalPlanner) (*Owner, error) {
	if queue == nil || inputs == nil || planner == nil {
		return nil, errs.New(errs.KindInternal, "Detach execution requires one queue and captured input owners")
	}
	return &Owner{queue: queue, inputs: inputs, planner: planner}, nil
}

func (owner *Owner) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item, task := request.Item.Item, request.Task.Task
	if item.Operation != workexecution.Detach || item.Input == nil || item.Phase != workexecution.Ready ||
		item.ActiveAttemptID != "" || at.IsZero() || at.Before(item.UpdatedAt) {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Detach cannot obtain this assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if request.Direction == workexecution.Recovery {
		return owner.admitRecovery(ctx, request, at)
	}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		change, err := owner.prepareQueuedStop(ctx, view, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	if request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Detach has no matching execution direction")
	}
	if request.DecisionOnly {
		if item.Placement.Executor != workexecution.Controller {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Detach decision cannot execute host work")
		}
		change, err := owner.inputs.PrepareDetachDecision(ctx, view, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	if item.Placement.Executor != workexecution.Agent || request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Detach requires its captured Agent host")
	}
	resource, err := owner.inputs.DetachResourceAdmission(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	consumer, err := owner.inputs.DetachConsumerAdmission(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	capture, err := owner.prepareCapture(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	capture.Conditions = append(capture.Conditions, resource.Conditions...)
	capture.Conditions = append(capture.Conditions, consumer.Conditions...)
	if len(capture.Mutations) != 0 {
		return workdispatch.Decision{Disposition: workdispatch.Advance, Change: capture}, nil
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: capture}, nil
}

func (owner *Owner) prepareCapture(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	current, err := owner.inputs.LoadDetachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer current.Clear()
	if current.Revision != 0 {
		return owner.inputs.DetachCaptureAdmission(ctx, view)
	}
	digest, err := attachremoval.Digest(current.Input.Snapshot.Source)
	if err != nil {
		return workqueue.Change{}, err
	}
	capture := attachremoval.WorkCapture{Schema: 1, TaskID: view.Task.Task.ID, WorkItemID: view.Item.Item.ID,
		OperationID: attachremoval.DetachOperation(view), Action: *view.Item.Item.Input, FrozenSHA256: digest,
		BackingProjectID: current.Input.Snapshot.Source.Attach.BackingProjectID}
	defer capture.Clear()
	source := current.Input.Snapshot.Source
	if attachremoval.NeedsAgent(source) && source.Backing.Desired.Adapter == "custom" {
		capture.Hook, err = owner.planner.CaptureHookInputs(ctx, capture.OperationID, source)
		if err != nil {
			return workqueue.Change{}, err
		}
		if capture.Hook != nil {
			capture.Sources = append([]tasksecretpinrecord.Record(nil), capture.Hook.SecretSources...)
		}
	}
	prepared := tasksecretpins.Prepared{}
	if len(capture.Sources) != 0 {
		prepared, err = owner.inputs.PrepareDetachPins(ctx, view, capture)
		if err != nil {
			return workqueue.Change{}, err
		}
		binding, err := prepared.Binding()
		if err != nil {
			return workqueue.Change{}, err
		}
		capture.Pins = &binding
	}
	return owner.inputs.PrepareDetachCapture(ctx, view, capture, prepared)
}

func (owner *Owner) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil || item.Item.Operation != workexecution.Detach || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach has no matching stop request")
	}
	if item.Item.ActiveAttemptID == "" && item.Item.Cleanup == workexecution.CleanupNone {
		view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
		if err != nil {
			return workqueue.Change{}, err
		}
		if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach stop ownership changed")
		}
		return owner.prepareQueuedStop(ctx, view, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *Owner) prepareQueuedStop(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	if view.Item.Item.AttemptNumber == 0 {
		return owner.inputs.PrepareDetachDecision(ctx, view, at)
	}
	_, previous, err := owner.queue.ReadAttempt(ctx, view.Task.Task.ID, view.Item.Item.ID, view.Item.Item.AttemptNumber, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach Retry has no settled preceding attempt")
	}
	outcome := workexecution.Failed
	if view.Task.Task.Stop != nil {
		outcome = view.Task.Task.Stop.Outcome
	} else if view.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	change, err := workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *previous.Attempt.Result, EffectsSettled: true}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	resource, err := owner.inputs.PrepareDetachFailed(ctx, view)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptKey(previous.Attempt.ID), ModRevision: previous.Revision})
	change.Mutations = append(change.Mutations, resource.Mutations...)
	return change, nil
}
