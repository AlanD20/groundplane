package workdispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (delivery *AgentDeliveries) removalMessage(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ControllerMessage, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		assignment, err := delivery.removals.LoadRemovalRecoveryAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "service removal recovery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ServiceRemovalRecoveryAssignment{
				ServiceRemovalRecoveryAssignment: assignment,
			},
		}, nil
	}
	assignment, err := delivery.removals.LoadWorkAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) ||
		attempt.Direction != workexecution.Forward {
		return nil, errs.New(errs.KindStateConflict, "service removal differs from its durable assignment")
	}
	return &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ServiceLifecycleAssignment{ServiceLifecycleAssignment: assignment},
	}, nil
}
