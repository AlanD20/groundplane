package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LifecycleReporter interface {
	RecoverLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleRecoveryReportRequest) (*agentpb.ServiceLifecycleRecoveryReportAck, error)
	DeliverLifecycleHook(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleHookRequest) (*agentpb.ServiceLifecycleHookDelivery, error)
	CheckpointLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleCheckpointRequest) (*agentpb.ServiceLifecycleCheckpointAck, error)
	CompleteLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleCompletionRequest) (*agentpb.ServiceLifecycleCompletionAck, error)
	FailLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleFailureRequest) (*agentpb.ServiceLifecycleFailureAck, error)
}

func (server *Server) ConfigureServiceLifecycle(reports LifecycleReporter) {
	server.lifecycleReports = reports
}

func clearLifecycleHookReply(message *agentpb.ControllerMessage) {
	hook := message.GetServiceLifecycleHookDelivery().GetHook()
	if hook == nil {
		return
	}
	for _, value := range hook.Inputs {
		if value != nil {
			clear(value.Value)
		}
	}
}

func (server *Server) lifecycleReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	if message.GetServiceLifecycleCheckpointRequest() == nil && message.GetServiceLifecycleCompletionRequest() == nil &&
		message.GetServiceLifecycleFailureRequest() == nil && message.GetServiceLifecycleHookRequest() == nil && message.GetServiceLifecycleRecoveryReportRequest() == nil {
		return false, nil, nil
	}
	if server.lifecycleReports == nil {
		return true, nil, status.Error(codes.Internal, "Service lifecycle reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_ServiceLifecycleRecoveryReportRequest:
		ack, reportErr := server.lifecycleReports.RecoverLifecycle(session.ctx, identity, payload.ServiceLifecycleRecoveryReportRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ServiceLifecycleRecoveryReportAck{ServiceLifecycleRecoveryReportAck: ack}
	case *agentpb.AgentMessage_ServiceLifecycleHookRequest:
		ack, reportErr := server.lifecycleReports.DeliverLifecycleHook(session.ctx, identity, payload.ServiceLifecycleHookRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ServiceLifecycleHookDelivery{ServiceLifecycleHookDelivery: ack}
	case *agentpb.AgentMessage_ServiceLifecycleCheckpointRequest:
		ack, reportErr := server.lifecycleReports.CheckpointLifecycle(session.ctx, identity, payload.ServiceLifecycleCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ServiceLifecycleCheckpointAck{ServiceLifecycleCheckpointAck: ack}
	case *agentpb.AgentMessage_ServiceLifecycleCompletionRequest:
		ack, reportErr := server.lifecycleReports.CompleteLifecycle(session.ctx, identity, payload.ServiceLifecycleCompletionRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ServiceLifecycleCompletionAck{ServiceLifecycleCompletionAck: ack}
	case *agentpb.AgentMessage_ServiceLifecycleFailureRequest:
		ack, reportErr := server.lifecycleReports.FailLifecycle(session.ctx, identity, payload.ServiceLifecycleFailureRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ServiceLifecycleFailureAck{ServiceLifecycleFailureAck: ack}

	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
