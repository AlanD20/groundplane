package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResolverReporter interface {
	CheckpointResolver(context.Context, agentregistration.WorkAuthorization, *agentpb.ResolverWorkCheckpointRequest) (*agentpb.ResolverWorkCheckpointAck, error)
	CompleteResolver(context.Context, agentregistration.WorkAuthorization, *agentpb.ResolverWorkCompletionRequest) (*agentpb.ResolverWorkResultAck, error)
	FailResolver(context.Context, agentregistration.WorkAuthorization, *agentpb.ResolverWorkFailureRequest) (*agentpb.ResolverWorkResultAck, error)
	RecoverResolver(context.Context, agentregistration.WorkAuthorization, *agentpb.ResolverWorkRecoveryRequest) (*agentpb.ResolverWorkResultAck, error)
}

func (server *Server) ConfigureResolverWork(reports ResolverReporter) {
	server.resolverReports = reports
}

func (server *Server) resolverReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	if message.GetResolverWorkCheckpointRequest() == nil && message.GetResolverWorkCompletionRequest() == nil &&
		message.GetResolverWorkFailureRequest() == nil && message.GetResolverWorkRecoveryRequest() == nil {
		return false, nil, nil
	}
	if server.resolverReports == nil {
		return true, nil, status.Error(codes.Internal, "DNS resolver reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_ResolverWorkCheckpointRequest:
		ack, reportErr := server.resolverReports.CheckpointResolver(session.ctx, identity, payload.ResolverWorkCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ResolverWorkCheckpointAck{ResolverWorkCheckpointAck: ack}
	case *agentpb.AgentMessage_ResolverWorkCompletionRequest:
		ack, reportErr := server.resolverReports.CompleteResolver(session.ctx, identity, payload.ResolverWorkCompletionRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ResolverWorkResultAck{ResolverWorkResultAck: ack}
	case *agentpb.AgentMessage_ResolverWorkFailureRequest:
		ack, reportErr := server.resolverReports.FailResolver(session.ctx, identity, payload.ResolverWorkFailureRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ResolverWorkResultAck{ResolverWorkResultAck: ack}
	case *agentpb.AgentMessage_ResolverWorkRecoveryRequest:
		ack, reportErr := server.resolverReports.RecoverResolver(session.ctx, identity, payload.ResolverWorkRecoveryRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_ResolverWorkResultAck{ResolverWorkResultAck: ack}
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
