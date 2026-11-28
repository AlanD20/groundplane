package workdispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RemoteImageDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.RemoteImageAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

func (delivery *AgentDeliveries) ConfigureRemoteImage(owner RemoteImageDeliveryOwner) error {
	if delivery == nil || owner == nil || delivery.remoteImage != nil {
		return errs.New(errs.KindInternal, "remote image Fetch delivery is missing or already configured")
	}
	delivery.remoteImage = owner
	return nil
}

func (delivery *AgentDeliveries) remoteImageAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ControllerMessage, error) {
	if delivery.remoteImage == nil {
		return nil, errs.New(errs.KindStateConflict, "remote image Fetch delivery is unavailable")
	}
	assignment, err := delivery.remoteImage.LoadWorkAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	attempt, _, err := remotefetch.DecodeAssignment(assignment, view.Attempt.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return nil, errs.New(errs.KindStateConflict, "remote image Fetch delivery differs from its captured assignment")
	}
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_RemoteImageAssignment{
		RemoteImageAssignment: assignment,
	}}, nil
}

func (delivery *AgentDeliveries) reconcileRemoteImage(
	ctx context.Context,
	attempt workexecution.Attempt,
	authorization agentregistration.WorkAuthorization,
) error {
	if delivery.remoteImage == nil {
		return errs.New(errs.KindStateConflict, "remote image Fetch reconciliation is unavailable")
	}
	view, err := delivery.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || view.Attempt.Attempt.FinishedAt != nil {
		return err
	}
	return delivery.remoteImage.ReconcileInterrupted(ctx, view, authorization)
}
