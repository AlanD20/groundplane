package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func openNetworkRecoveryHelper(request *agentpb.NetworkWorkHelperRequest) (NetworkHelperInput, error) {
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil ||
		!request.InspectOnly || request.Report != nil || request.Acknowledgement != nil ||
		request.Assignment == nil || request.Assignment.Attempt == nil {
		return NetworkHelperInput{}, invalidNetworkWork()
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
		return NetworkHelperInput{}, err
	}
	procedure, source, boundary, err := OpenNetworkRecoveryInput(attempt, request.Assignment)
	if err != nil {
		return NetworkHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return NetworkHelperInput{}, err
	}
	attempt.StartedAt = &started
	return NetworkHelperInput{
		Attempt:     attempt,
		Procedure:   procedure,
		Progress:    boundary,
		InspectOnly: true,
		Recovery:    &source,
	}, nil
}
