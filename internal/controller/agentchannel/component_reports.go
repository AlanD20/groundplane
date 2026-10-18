package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/componentexecution"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ComponentReporter interface {
	LoadComponentConfiguration(context.Context, agentregistration.WorkAuthorization, *agentpb.ComponentConfigurationRequest) (*agentpb.ComponentConfigurationManifest, error)
	LoadComponentConfigurationFile(context.Context, agentregistration.WorkAuthorization, *agentpb.ComponentConfigurationFileRequest) (*agentpb.ComponentConfigurationFileDelivery, error)
	CheckpointComponent(context.Context, agentregistration.WorkAuthorization, *agentpb.ComponentWorkCheckpointRequest) (*agentpb.ComponentWorkCheckpointAck, error)
	CompleteComponent(context.Context, agentregistration.WorkAuthorization, *agentpb.ComponentWorkCompletionRequest) (*agentpb.ComponentWorkCompletionAck, error)
	FailComponent(context.Context, agentregistration.WorkAuthorization, *agentpb.ComponentWorkFailureRequest) (*agentpb.ComponentWorkFailureAck, error)
	RecoverComponent(context.Context, agentregistration.WorkAuthorization, *agentpb.ComponentWorkRecoveryReportRequest) (*agentpb.ComponentWorkRecoveryReportAck, error)
}

func (server *Server) ConfigureComponentWork(reports ComponentReporter) {
	server.componentReports = reports
}

func (server *Server) componentReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	if message.GetComponentWorkCheckpointRequest() == nil && message.GetComponentWorkCompletionRequest() == nil &&
		message.GetComponentWorkFailureRequest() == nil && message.GetComponentWorkRecoveryReportRequest() == nil &&
		message.GetComponentConfigurationRequest() == nil && message.GetComponentConfigurationFileRequest() == nil {
		return false, nil, nil
	}
	if server.componentReports == nil {
		return true, nil, status.Error(codes.Internal, "Component reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_ComponentConfigurationRequest:
		manifest, loadErr := server.componentReports.LoadComponentConfiguration(session.ctx, identity, payload.ComponentConfigurationRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_ComponentConfigurationManifest{ComponentConfigurationManifest: manifest}
	case *agentpb.AgentMessage_ComponentConfigurationFileRequest:
		delivery, loadErr := server.componentReports.LoadComponentConfigurationFile(session.ctx, identity, payload.ComponentConfigurationFileRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_ComponentConfigurationFileDelivery{ComponentConfigurationFileDelivery: delivery}
	case *agentpb.AgentMessage_ComponentWorkCheckpointRequest:
		ack, reportErr := server.componentReports.CheckpointComponent(session.ctx, identity, payload.ComponentWorkCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ComponentWorkCheckpointAck{ComponentWorkCheckpointAck: ack}
	case *agentpb.AgentMessage_ComponentWorkCompletionRequest:
		ack, reportErr := server.componentReports.CompleteComponent(session.ctx, identity, payload.ComponentWorkCompletionRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ComponentWorkCompletionAck{ComponentWorkCompletionAck: ack}
	case *agentpb.AgentMessage_ComponentWorkFailureRequest:
		ack, reportErr := server.componentReports.FailComponent(session.ctx, identity, payload.ComponentWorkFailureRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ComponentWorkFailureAck{ComponentWorkFailureAck: ack}
	case *agentpb.AgentMessage_ComponentWorkRecoveryReportRequest:
		ack, reportErr := server.componentReports.RecoverComponent(session.ctx, identity, payload.ComponentWorkRecoveryReportRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ComponentWorkRecoveryReportAck{ComponentWorkRecoveryReportAck: ack}
	}
	if err != nil {
		componentexecution.ClearConfigurationDelivery(reply.GetComponentConfigurationFileDelivery())
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
