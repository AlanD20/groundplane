package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BackingReporter interface {
	CheckpointBackingProvision(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackingProvisionCheckpointRequest,
	) (*agentpb.BackingProvisionCheckpointAck, error)
	CompleteBackingProvision(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackingProvisionCompletionRequest,
	) (*agentpb.BackingProvisionCompletionAck, error)
	FailBackingProvision(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackingProvisionFailureRequest,
	) (*agentpb.BackingProvisionFailureAck, error)
	RecoverBackingProvision(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackingProvisionRecoveryRequest,
	) (*agentpb.BackingProvisionRecoveryAck, error)
	LoadBackingProvisionConfiguration(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackingProvisionConfigurationRequest,
	) (*agentpb.BackingProvisionConfigurationManifest, error)
	LoadBackingProvisionFile(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackingProvisionFileRequest,
	) (*agentpb.BackingProvisionFileDelivery, error)
	DeliverBackingProvisionHook(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackingProvisionHookRequest,
	) (*agentpb.BackingProvisionHookDelivery, error)
}

func (server *Server) ConfigureBackingWork(reports BackingReporter) {
	server.backingReports = reports
}

func (server *Server) backingReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	if message.GetBackingProvisionCheckpointRequest() == nil &&
		message.GetBackingProvisionConfigurationRequest() == nil &&
		message.GetBackingProvisionFileRequest() == nil &&
		message.GetBackingProvisionHookRequest() == nil {
		return false, nil, nil
	}
	if server.backingReports == nil {
		return true, nil, status.Error(codes.Internal, "backing creation reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_BackingProvisionCheckpointRequest:
		ack, reportErr := server.backingReports.CheckpointBackingProvision(session.ctx, identity, payload.BackingProvisionCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_BackingProvisionCheckpointAck{BackingProvisionCheckpointAck: ack}
	case *agentpb.AgentMessage_BackingProvisionConfigurationRequest:
		manifest, loadErr := server.backingReports.LoadBackingProvisionConfiguration(session.ctx, identity, payload.BackingProvisionConfigurationRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_BackingProvisionConfigurationManifest{BackingProvisionConfigurationManifest: manifest}
	case *agentpb.AgentMessage_BackingProvisionFileRequest:
		delivery, loadErr := server.backingReports.LoadBackingProvisionFile(session.ctx, identity, payload.BackingProvisionFileRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_BackingProvisionFileDelivery{BackingProvisionFileDelivery: delivery}
	case *agentpb.AgentMessage_BackingProvisionHookRequest:
		delivery, loadErr := server.backingReports.DeliverBackingProvisionHook(session.ctx, identity, payload.BackingProvisionHookRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_BackingProvisionHookDelivery{BackingProvisionHookDelivery: delivery}
	}
	if err != nil {
		backingprovision.ClearPrivateDelivery(reply)
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
