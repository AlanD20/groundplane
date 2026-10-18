package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/entryexecution"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EntryReporter interface {
	RecoverEntry(context.Context, agentregistration.WorkAuthorization, *agentpb.EntryConfigurationWorkRecoveryReportRequest) (*agentpb.EntryConfigurationWorkRecoveryReportAck, error)
	LoadEntryConfiguration(context.Context, agentregistration.WorkAuthorization, *agentpb.EntryConfigurationRequest) (*agentpb.EntryConfigurationManifest, error)
	LoadEntryConfigurationFile(context.Context, agentregistration.WorkAuthorization, *agentpb.EntryConfigurationFileRequest) (*agentpb.EntryConfigurationFileDelivery, error)
	CheckpointEntry(context.Context, agentregistration.WorkAuthorization, *agentpb.EntryConfigurationWorkCheckpointRequest) (*agentpb.EntryConfigurationWorkCheckpointAck, error)
	CompleteEntry(context.Context, agentregistration.WorkAuthorization, *agentpb.EntryConfigurationWorkCompletionRequest) (*agentpb.EntryConfigurationWorkCompletionAck, error)
	FailEntry(context.Context, agentregistration.WorkAuthorization, *agentpb.EntryConfigurationWorkFailureRequest) (*agentpb.EntryConfigurationWorkFailureAck, error)
}

func (server *Server) ConfigureEntryConfigurationWork(reports EntryReporter) {
	server.entryReports = reports
}

func (server *Server) entryReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	if message.GetEntryConfigurationWorkCheckpointRequest() == nil && message.GetEntryConfigurationWorkCompletionRequest() == nil &&
		message.GetEntryConfigurationWorkFailureRequest() == nil && message.GetEntryConfigurationWorkRecoveryReportRequest() == nil &&
		message.GetEntryConfigurationRequest() == nil && message.GetEntryConfigurationFileRequest() == nil {
		return false, nil, nil
	}
	if server.entryReports == nil {
		return true, nil, status.Error(codes.Internal, "Entry reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_EntryConfigurationWorkRecoveryReportRequest:
		ack, reportErr := server.entryReports.RecoverEntry(session.ctx, identity, payload.EntryConfigurationWorkRecoveryReportRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_EntryConfigurationWorkRecoveryReportAck{EntryConfigurationWorkRecoveryReportAck: ack}
	case *agentpb.AgentMessage_EntryConfigurationRequest:
		manifest, loadErr := server.entryReports.LoadEntryConfiguration(session.ctx, identity, payload.EntryConfigurationRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_EntryConfigurationManifest{EntryConfigurationManifest: manifest}
	case *agentpb.AgentMessage_EntryConfigurationFileRequest:
		delivery, loadErr := server.entryReports.LoadEntryConfigurationFile(session.ctx, identity, payload.EntryConfigurationFileRequest)
		err, reply.Payload = loadErr, &agentpb.ControllerMessage_EntryConfigurationFileDelivery{EntryConfigurationFileDelivery: delivery}
	case *agentpb.AgentMessage_EntryConfigurationWorkCheckpointRequest:
		ack, reportErr := server.entryReports.CheckpointEntry(session.ctx, identity, payload.EntryConfigurationWorkCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_EntryConfigurationWorkCheckpointAck{EntryConfigurationWorkCheckpointAck: ack}
	case *agentpb.AgentMessage_EntryConfigurationWorkCompletionRequest:
		ack, reportErr := server.entryReports.CompleteEntry(session.ctx, identity, payload.EntryConfigurationWorkCompletionRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_EntryConfigurationWorkCompletionAck{EntryConfigurationWorkCompletionAck: ack}
	case *agentpb.AgentMessage_EntryConfigurationWorkFailureRequest:
		ack, reportErr := server.entryReports.FailEntry(session.ctx, identity, payload.EntryConfigurationWorkFailureRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_EntryConfigurationWorkFailureAck{EntryConfigurationWorkFailureAck: ack}

	}
	if err != nil {
		entryexecution.ClearConfigurationDelivery(reply.GetEntryConfigurationFileDelivery())
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
