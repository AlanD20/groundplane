package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AttachProvisionReporter interface {
	ReportProvisionRecovery(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachProvisionRecoveryReportRequest) (*agentpb.AttachProvisionRecoveryReportAck, error)
	FailAttachProvision(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachProvisionFailureRequest) (*agentpb.AttachProvisionFailureAck, error)
	CompleteAttachProvision(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachProvisionCompletionRequest) (*agentpb.AttachProvisionCompletionAck, error)
	CheckpointAttachProvision(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachProvisionCheckpointRequest) (*agentpb.AttachProvisionCheckpointAck, error)
	StoreAttachProvisionFacts(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachProvisionFactsRequest) (*agentpb.AttachProvisionFactsAck, error)
}

func (server *Server) ConfigureAttachProvision(reports AttachProvisionReporter) {
	server.attachProvisionReports = reports
}

func (server *Server) attachProvisionReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	if message.GetAttachProvisionCheckpointRequest() == nil && message.GetAttachProvisionFactsRequest() == nil && message.GetAttachProvisionCompletionRequest() == nil && message.GetAttachProvisionFailureRequest() == nil && message.GetAttachProvisionRecoveryReportRequest() == nil {
		return false, nil, nil
	}
	if facts := message.GetAttachProvisionFactsRequest(); facts != nil {
		defer attachexecution.ClearProvisionFacts(facts)
	}
	if server.attachProvisionReports == nil {
		return true, nil, status.Error(codes.Internal, "Attach provisioning reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_AttachProvisionRecoveryReportRequest:
		ack, reportErr := server.attachProvisionReports.ReportProvisionRecovery(session.ctx, identity, payload.AttachProvisionRecoveryReportRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachProvisionRecoveryReportAck{AttachProvisionRecoveryReportAck: ack}
	case *agentpb.AgentMessage_AttachProvisionFailureRequest:
		ack, reportErr := server.attachProvisionReports.FailAttachProvision(session.ctx, identity, payload.AttachProvisionFailureRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachProvisionFailureAck{AttachProvisionFailureAck: ack}
	case *agentpb.AgentMessage_AttachProvisionCompletionRequest:
		ack, reportErr := server.attachProvisionReports.CompleteAttachProvision(session.ctx, identity, payload.AttachProvisionCompletionRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachProvisionCompletionAck{AttachProvisionCompletionAck: ack}
	case *agentpb.AgentMessage_AttachProvisionCheckpointRequest:
		ack, reportErr := server.attachProvisionReports.CheckpointAttachProvision(session.ctx, identity, payload.AttachProvisionCheckpointRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachProvisionCheckpointAck{AttachProvisionCheckpointAck: ack}
	case *agentpb.AgentMessage_AttachProvisionFactsRequest:
		ack, reportErr := server.attachProvisionReports.StoreAttachProvisionFacts(session.ctx, identity, payload.AttachProvisionFactsRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachProvisionFactsAck{AttachProvisionFactsAck: ack}
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
