package detachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *Owner) admitRecovery(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if request.DecisionOnly || item.Placement.Executor != workexecution.Agent ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Detach recovery requires its captured host and unresolved effects")
	}
	if !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	change, err := owner.recoveryAdmission(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: change}, nil
}

func (owner *Owner) recoveryAdmission(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	change, err := owner.inputs.DetachCaptureAdmission(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	source, err := owner.inputs.LoadDetachRecovery(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	resource, err := owner.inputs.PrepareDetachFailed(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(resource.Mutations) != 0 {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach original failure has not settled its resource metadata")
	}
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: attachremoval.WorkRecoveryKey(view.Item.Item.ID), ModRevision: source.Revision})
	return change, nil
}

func (owner *Owner) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !view.CurrentExecutor || !view.ActiveAttempt || attempt.StartedAt != nil || item.Operation != workexecution.Detach ||
		workexecution.ValidateDelivery(item, attempt) != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach no longer owns a new start grant")
	}
	if attempt.Direction == workexecution.Recovery {
		return owner.recoveryAdmission(ctx, view.Work)
	}
	if task.Stop != nil || item.ForwardRevoked {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach forward execution was stopped")
	}
	change, err := owner.inputs.DetachResourceAdmission(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	capture, err := owner.inputs.DetachCaptureAdmission(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	consumer, err := owner.inputs.DetachConsumerAdmission(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture.Conditions...)
	change.Conditions = append(change.Conditions, consumer.Conditions...)
	return change, nil
}

func (owner *Owner) DetachAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ControllerMessage, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !view.CurrentExecutor || !view.ActiveAttempt || item.Operation != workexecution.Detach ||
		workexecution.ValidateDelivery(item, attempt) != nil || task.CollectionStartedAt != nil || task.FinishedAt != nil ||
		!task.PublicationComplete {
		return nil, errs.New(errs.KindStateConflict, "Detach delivery no longer owns active execution")
	}
	if attempt.Direction == workexecution.Recovery {
		input, _, err := owner.inputs.DetachRecoveryInput(ctx, view.Work, attempt)
		if err != nil {
			return nil, err
		}
		defer attachexecution.ClearRecoveryInput(&input)
		assignment, err := attachexecution.EncodeRecoveryAssignment(input)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachRecoveryAssignment{AttachRecoveryAssignment: assignment}}, nil
	}
	input, err := owner.forwardInput(ctx, view)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearRemoval(input.Removal)
	assignment, err := attachexecution.EncodeRemovalAssignment(input)
	if err != nil {
		return nil, err
	}
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_AttachRemovalAssignment{AttachRemovalAssignment: assignment}}, nil
}

// forwardInput reconstructs only the immutable authority. Reconciliation may
// use it for a replaced process without giving that process a new grant.
func (owner *Owner) forwardInput(ctx context.Context, view workqueue.ResumeView) (attachexecution.RemovalInput, error) {
	if view.Work.Item.Item.Operation != workexecution.Detach || view.Attempt.Attempt.Direction != workexecution.Forward {
		return attachexecution.RemovalInput{}, errs.New(errs.KindStateConflict, "Detach has no matching captured forward attempt")
	}
	capture, err := owner.inputs.LoadDetachCapture(ctx, view.Work)
	if err != nil {
		return attachexecution.RemovalInput{}, err
	}
	defer capture.Clear()
	if capture.Revision <= 0 {
		return attachexecution.RemovalInput{}, errs.New(errs.KindStateConflict, "Detach command inputs have not been captured")
	}
	return owner.planner.ResolveDetachInput(ctx, capture.Input, capture.Capture, view.Attempt.Attempt)
}
