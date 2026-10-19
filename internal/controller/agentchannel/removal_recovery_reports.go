package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RemovalRecoveryReporter interface {
	LoadRemovalConfiguration(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.ServiceRemovalConfigurationRequest,
	) (*agentpb.ServiceRemovalConfigurationManifest, error)
	LoadRemovalConfigurationFile(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.ServiceRemovalConfigurationFileRequest,
	) (*agentpb.ServiceRemovalConfigurationFileDelivery, error)
	CheckpointRemovalRecovery(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.ServiceRemovalRecoveryCheckpointRequest,
	) (*agentpb.ServiceRemovalRecoveryCheckpointAck, error)
	CompleteRemovalRecovery(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.ServiceRemovalRecoveryCompletionRequest,
	) (*agentpb.ServiceRemovalRecoveryCompletionAck, error)
	FailRemovalRecovery(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.ServiceRemovalRecoveryFailureRequest,
	) (*agentpb.ServiceRemovalRecoveryFailureAck, error)
}

func (server *Server) ConfigureServiceRemovalRecovery(reports RemovalRecoveryReporter) {
	server.removalRecoveryReports = reports
}

func (server *Server) removalRecoveryReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	if message.GetServiceRemovalConfigurationRequest() == nil &&
		message.GetServiceRemovalConfigurationFileRequest() == nil &&
		message.GetServiceRemovalRecoveryCheckpointRequest() == nil {
		return false, nil, nil
	}
	if server.removalRecoveryReports == nil {
		return true, nil, status.Error(codes.Internal, "Service removal recovery reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_ServiceRemovalConfigurationRequest:
		manifest, loadErr := server.removalRecoveryReports.LoadRemovalConfiguration(session.ctx, identity, payload.ServiceRemovalConfigurationRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_ServiceRemovalConfigurationManifest{ServiceRemovalConfigurationManifest: manifest}
	case *agentpb.AgentMessage_ServiceRemovalConfigurationFileRequest:
		delivery, loadErr := server.removalRecoveryReports.LoadRemovalConfigurationFile(session.ctx, identity, payload.ServiceRemovalConfigurationFileRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_ServiceRemovalConfigurationFileDelivery{ServiceRemovalConfigurationFileDelivery: delivery}
	case *agentpb.AgentMessage_ServiceRemovalRecoveryCheckpointRequest:
		ack, reportErr := server.removalRecoveryReports.CheckpointRemovalRecovery(session.ctx, identity, payload.ServiceRemovalRecoveryCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ServiceRemovalRecoveryCheckpointAck{ServiceRemovalRecoveryCheckpointAck: ack}
	}
	if err != nil {
		if delivery := reply.GetServiceRemovalConfigurationFileDelivery(); delivery != nil {
			clear(delivery.Content)
			delivery.Content = nil
		}
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
