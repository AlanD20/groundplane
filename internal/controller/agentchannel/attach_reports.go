package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AttachReporter interface {
	FailAttachRemoval(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachRemovalFailureRequest) (*agentpb.AttachRemovalFailureAck, error)
	CompleteAttachRemoval(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachRemovalCompletionRequest) (*agentpb.AttachRemovalCompletionAck, error)
	CheckpointAttachRemoval(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachRemovalCheckpointRequest) (*agentpb.AttachRemovalCheckpointAck, error)
	CompleteAttachRecovery(context.Context, agentregistration.WorkAuthorization, *agentpb.AttachRecoveryCompletionRequest) (*agentpb.AttachRecoveryCompletionAck, error)
}

func (server *Server) ConfigureAttachWork(reports AttachReporter) { server.attachReports = reports }

// attachReport keeps typed Attach reporting on the authenticated session. The
// stream loop owns transport; operation owners own evidence and settlement.
func (server *Server) attachReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	if message.GetAttachRemovalFailureRequest() == nil && message.GetAttachRemovalCompletionRequest() == nil &&
		message.GetAttachRemovalCheckpointRequest() == nil && message.GetAttachRecoveryCompletionRequest() == nil {
		return false, nil, nil
	}
	if server.attachReports == nil {
		return true, nil, status.Error(codes.Internal, "Attach reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_AttachRemovalFailureRequest:
		ack, reportErr := server.attachReports.FailAttachRemoval(session.ctx, identity, payload.AttachRemovalFailureRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachRemovalFailureAck{AttachRemovalFailureAck: ack}
	case *agentpb.AgentMessage_AttachRemovalCompletionRequest:
		ack, reportErr := server.attachReports.CompleteAttachRemoval(session.ctx, identity, payload.AttachRemovalCompletionRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachRemovalCompletionAck{AttachRemovalCompletionAck: ack}
	case *agentpb.AgentMessage_AttachRemovalCheckpointRequest:
		ack, reportErr := server.attachReports.CheckpointAttachRemoval(session.ctx, identity, payload.AttachRemovalCheckpointRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachRemovalCheckpointAck{AttachRemovalCheckpointAck: ack}
	case *agentpb.AgentMessage_AttachRecoveryCompletionRequest:
		ack, reportErr := server.attachReports.CompleteAttachRecovery(session.ctx, identity, payload.AttachRecoveryCompletionRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachRecoveryCompletionAck{AttachRecoveryCompletionAck: ack}
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
