package workdispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type PublicIngressDeliveryOwner interface {
	LoadWorkAssignment(context.Context, workqueue.ResumeView) (*agentpb.PublicIngressAssignment, error)
	ReconcileInterrupted(context.Context, workqueue.ResumeView, agentregistration.WorkAuthorization) error
}

func (delivery *AgentDeliveries) ConfigurePublicIngress(owner PublicIngressDeliveryOwner) error {
	if delivery == nil || owner == nil || delivery.publicIngress != nil {
		return errs.New(errs.KindInternal, "public ingress delivery is missing or already configured")
	}
	delivery.publicIngress = owner
	return nil
}

func (delivery *AgentDeliveries) publicIngressAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ControllerMessage, error) {
	if delivery.publicIngress == nil {
		return nil, errs.New(errs.KindStateConflict, "public ingress delivery is unavailable")
	}
	assignment, err := delivery.publicIngress.LoadWorkAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	attempt, _, err := ingressexecution.DecodeAssignment(assignment, view.Attempt.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return nil, errs.New(errs.KindStateConflict, "public ingress delivery differs from its captured assignment")
	}
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_PublicIngressAssignment{
		PublicIngressAssignment: assignment,
	}}, nil
}

func (delivery *AgentDeliveries) reconcilePublicIngress(
	ctx context.Context,
	attempt workexecution.Attempt,
	authorization agentregistration.WorkAuthorization,
) error {
	if delivery.publicIngress == nil {
		return errs.New(errs.KindStateConflict, "public ingress reconciliation is unavailable")
	}
	view, err := delivery.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || view.Attempt.Attempt.FinishedAt != nil {
		return err
	}
	return delivery.publicIngress.ReconcileInterrupted(ctx, view, authorization)
}
