package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AttachReporter interface {
	FailAttachRemoval(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.AttachRemovalFailureRequest,
	) (*agentpb.AttachRemovalFailureAck, error)
	CompleteAttachRemoval(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.AttachRemovalCompletionRequest,
	) (*agentpb.AttachRemovalCompletionAck, error)
	CheckpointAttachRemoval(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.AttachRemovalCheckpointRequest,
	) (*agentpb.AttachRemovalCheckpointAck, error)
	CompleteAttachRecovery(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.AttachRecoveryCompletionRequest,
	) (*agentpb.AttachRecoveryCompletionAck, error)
}

func (server *Server) ConfigureAttachWork(reports AttachReporter) { server.attachReports = reports }

// attachReport keeps typed Attach reporting on the authenticated session. The
// stream loop owns transport; operation owners own evidence and settlement.
func (server *Server) attachReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	if handled, reply, err := server.attachProvisionReport(session, generation, message); handled {
		return handled, reply, err
	}
	if message.GetAttachRemovalCheckpointRequest() == nil {
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
	case *agentpb.AgentMessage_AttachRemovalCheckpointRequest:
		ack, reportErr := server.attachReports.CheckpointAttachRemoval(session.ctx, identity, payload.AttachRemovalCheckpointRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_AttachRemovalCheckpointAck{AttachRemovalCheckpointAck: ack}
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
