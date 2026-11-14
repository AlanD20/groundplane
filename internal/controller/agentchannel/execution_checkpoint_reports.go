package agentchannel

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These operation owners share the same authenticated, request-local retry and
// rejection boundary as the other native checkpoints.
func (server *Server) executionCheckpointReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	switch message.Payload.(type) {
	case *agentpb.AgentMessage_ReleaseWorkCheckpointRequest,
		*agentpb.AgentMessage_ReleaseRecoveryCheckpointRequest,
		*agentpb.AgentMessage_EnvironmentRemovalCheckpointRequest,
		*agentpb.AgentMessage_ScriptWorkCheckpointRequest:
	default:
		return false, nil, nil
	}
	authorization, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_ReleaseWorkCheckpointRequest:
		if server.releaseCheckpoints == nil {
			return true, nil, status.Error(codes.Internal, "Release checkpoint service is not configured")
		}
		ack, reportErr := server.releaseCheckpoints.CheckpointRelease(session.ctx, authorization, payload.ReleaseWorkCheckpointRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_ReleaseWorkCheckpointAck{ReleaseWorkCheckpointAck: ack}
	case *agentpb.AgentMessage_ReleaseRecoveryCheckpointRequest:
		if server.releaseCheckpoints == nil {
			return true, nil, status.Error(codes.Internal, "Release recovery checkpoint service is not configured")
		}
		ack, reportErr := server.releaseCheckpoints.CheckpointRecovery(session.ctx, authorization, payload.ReleaseRecoveryCheckpointRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_ReleaseRecoveryCheckpointAck{ReleaseRecoveryCheckpointAck: ack}
	case *agentpb.AgentMessage_EnvironmentRemovalCheckpointRequest:
		if server.hierarchyReports == nil {
			return true, nil, status.Error(codes.Internal, "Environment removal reporting is not configured")
		}
		ack, reportErr := server.hierarchyReports.CheckpointEnvironmentRemoval(session.ctx, authorization, payload.EnvironmentRemovalCheckpointRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_EnvironmentRemovalCheckpointAck{EnvironmentRemovalCheckpointAck: ack}
	case *agentpb.AgentMessage_ScriptWorkCheckpointRequest:
		ack, reportErr := server.checkpointScriptWork(session.ctx, authorization, payload.ScriptWorkCheckpointRequest)
		err = reportErr
		reply.Payload = &agentpb.ControllerMessage_ScriptWorkCheckpointAck{ScriptWorkCheckpointAck: ack}
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
