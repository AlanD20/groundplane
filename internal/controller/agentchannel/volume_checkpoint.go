package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ScriptCheckpointer interface {
	CheckpointScript(
		context.Context,
		string,
		uint64,
		*agentpb.ScriptCheckpointRequest,
	) (*agentpb.ScriptCheckpointAck, error)
}

type VolumeRemovalCheckpointer interface {
	CheckpointVolumeRemoval(
		context.Context,
		string,
		uint64,
		*agentpb.VolumeRemovalCheckpointRequest,
	) (*agentpb.VolumeRemovalCheckpointAck, error)
}

func (s *Server) checkpointVolumeRemoval(ctx context.Context, agentID string, generation uint64,
	request *agentpb.VolumeRemovalCheckpointRequest,
) (*agentpb.ControllerMessage, error) {
	if s.volumeCheckpoints == nil {
		return nil, errs.New(errs.KindInternal, "Volume removal checkpoint service is not configured")
	}
	ack, err := s.volumeCheckpoints.CheckpointVolumeRemoval(ctx, agentID, generation, request)
	if err != nil {
		return nil, err
	}
	return &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_VolumeRemovalCheckpointAck{VolumeRemovalCheckpointAck: ack},
	}, nil
}
