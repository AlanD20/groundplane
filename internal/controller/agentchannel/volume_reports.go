package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type VolumeReporter interface {
	CheckpointVolume(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.VolumeWorkCheckpointRequest,
	) (*agentpb.VolumeWorkCheckpointAck, error)
	CompleteVolume(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.VolumeWorkCompletionRequest,
	) (*agentpb.VolumeWorkCompletionAck, error)
	FailVolume(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.VolumeWorkFailureRequest,
	) (*agentpb.VolumeWorkFailureAck, error)
	RecoverVolume(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.VolumeWorkRecoveryReportRequest,
	) (*agentpb.VolumeWorkRecoveryReportAck, error)
}

func (server *Server) ConfigureVolumeWork(reports VolumeReporter) { server.volumeReports = reports }

func (server *Server) volumeReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	if message.GetVolumeWorkCheckpointRequest() == nil {
		return false, nil, nil
	}
	if server.volumeReports == nil {
		return true, nil, status.Error(codes.Internal, "Volume reporting is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	reply := &agentpb.ControllerMessage{}
	switch payload := message.Payload.(type) {
	case *agentpb.AgentMessage_VolumeWorkCheckpointRequest:
		ack, reportErr := server.volumeReports.CheckpointVolume(session.ctx, identity, payload.VolumeWorkCheckpointRequest)
		err, reply.Payload = reportErr, &agentpb.ControllerMessage_VolumeWorkCheckpointAck{VolumeWorkCheckpointAck: ack}
	}
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, reply, nil
}
