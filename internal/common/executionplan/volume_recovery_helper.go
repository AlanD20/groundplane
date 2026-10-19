package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func openVolumeRecoveryHelper(request *agentpb.VolumeWorkHelperRequest) (VolumeHelperInput, error) {
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil ||
		!request.InspectOnly || request.Report != nil || request.Acknowledgement != nil || len(request.DirectoryBatch) != 0 ||
		request.Assignment == nil || request.Assignment.Attempt == nil {
		return VolumeHelperInput{}, invalidVolumeWork()
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     claimed.HostId,
		AgentID:    claimed.AgentId,
		Generation: claimed.ExecutorGeneration,
	}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	procedure, source, boundary, err := OpenVolumeRecoveryInput(attempt, request.Assignment)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	attempt.StartedAt = &started
	return VolumeHelperInput{
		Attempt:     attempt,
		Procedure:   procedure,
		Progress:    boundary,
		InspectOnly: true,
		Recovery:    &source,
	}, nil
}
