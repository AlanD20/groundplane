package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NetworkReporter interface {
	CheckpointNetwork(context.Context, agentregistration.WorkAuthorization, *agentpb.NetworkWorkCheckpointRequest) (*agentpb.NetworkWorkCheckpointAck, error)
	CompleteNetwork(context.Context, agentregistration.WorkAuthorization, *agentpb.NetworkWorkCompletionRequest) (*agentpb.NetworkWorkCompletionAck, error)
	FailNetwork(context.Context, agentregistration.WorkAuthorization, *agentpb.NetworkWorkFailureRequest) (*agentpb.NetworkWorkFailureAck, error)
	RecoverNetwork(context.Context, agentregistration.WorkAuthorization, *agentpb.NetworkWorkRecoveryReportRequest) (*agentpb.NetworkWorkRecoveryReportAck, error)
}

func (server *Server) ConfigureNetworkWork(reports NetworkReporter) { server.networkReports = reports }

func (server *Server) networkReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	if message.GetNetworkWorkCheckpointRequest() == nil {
		return false, nil, nil
	}
	if server.networkReports == nil {
		return true, nil, status.Error(codes.Internal, "Network reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_NetworkWorkCheckpointRequest:
		ack, reportErr := server.networkReports.CheckpointNetwork(session.ctx, identity, payload.NetworkWorkCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_NetworkWorkCheckpointAck{NetworkWorkCheckpointAck: ack}
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
