package hierarchydeletion

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// HierarchyAssignment dispatches the closed captured action, never inferring
// operation type from the current resource or creating a hidden child Task.
func (owner *AgentWork) HierarchyAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ControllerMessage, error) {
	item, attempt := view.Work.Item.Item, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || item.Operation != workexecution.Hierarchy ||
		workexecution.ValidateDelivery(item, attempt) != nil || view.Work.Task.Task.CollectionStartedAt != nil ||
		view.Work.Task.Task.FinishedAt != nil {
		return nil, errs.New(errs.KindStateConflict, "removal assignment is no longer active")
	}
	captured, err := owner.records.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	switch captured.Action.ActionKind {
	case hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup:
		assignment, err := owner.capturedEnvironmentAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_EnvironmentRemovalAssignment{EnvironmentRemovalAssignment: assignment},
		}, nil
	case hierarchydeletion.HierarchyDeletionAttachDeprovision:
		if attempt.Direction == workexecution.Recovery {
			source, err := owner.records.LoadHierarchyAttachRecovery(ctx, view.Work)
			if err != nil {
				return nil, err
			}
			assignment, err := attachexecution.EncodeRecoveryAssignment(
				attachexecution.RecoveryInput{Attempt: attempt, Source: source.Source},
			)
			if err != nil {
				return nil, err
			}
			return &agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_AttachRecoveryAssignment{AttachRecoveryAssignment: assignment},
			}, nil
		}
		assignment, err := owner.capturedAttachAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_AttachRemovalAssignment{AttachRemovalAssignment: assignment},
		}, nil
	default:
		return nil, errs.New(errs.KindStateConflict, "captured removal action has no host execution handler")
	}
}

func (owner *AgentWork) capturedAttachAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.AttachRemovalAssignment, error) {
	input, err := owner.capturedAttachInput(ctx, view)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearRemoval(input.Removal)
	return attachexecution.EncodeRemovalAssignment(input)
}

func (owner *AgentWork) capturedAttachInput(
	ctx context.Context,
	view workqueue.ResumeView,
) (attachexecution.RemovalInput, error) {
	capture, err := owner.records.LoadHierarchyAttachCapture(ctx, view.Work)
	if err != nil {
		return attachexecution.RemovalInput{}, err
	}
	defer capture.Clear()
	if capture.Revision <= 0 {
		return attachexecution.RemovalInput{}, errs.New(
			errs.KindStateConflict,
			"Attach removal inputs have not been captured",
		)
	}
	raw, err := attachremoval.EncodeWorkCapture(capture.Capture)
	if err != nil {
		return attachexecution.RemovalInput{}, err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	input, err := owner.attachPlanner.Build(ctx, capture.Source.Input, &agentpb.WorkInputReference{
		InputId: ids.DeriveAt(
			ids.KindPlan,
			view.Work.Task.Task.CreatedAt,
			view.Work.Item.Item.ID,
			"attach-removal-source",
		), Sha256: digest[:]}, nil)
	if err != nil {
		return attachexecution.RemovalInput{}, err
	}
	return attachexecution.RemovalInput{Attempt: view.Attempt.Attempt, Removal: input}, nil
}
