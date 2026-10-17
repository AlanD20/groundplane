package executionplan

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// NetworkHelperInput is opened only after all captured content and receipts
// agree. The helper transport/launcher authenticates who supplied this envelope;
// the self-described Agent identity is not an authentication credential.
type NetworkHelperInput struct {
	Attempt     workexecution.Attempt
	Procedure   *agentpb.NetworkWorkInput
	Progress    networkexecution.Progress
	InspectOnly bool
}

func OpenNetworkHelperRequest(request *agentpb.NetworkWorkHelperRequest) (NetworkHelperInput, error) {
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil ||
		request.Assignment == nil || request.Assignment.Attempt == nil || request.Report == nil || request.Acknowledgement == nil {
		return NetworkHelperInput{}, invalidNetworkWork()
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId,
		AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return NetworkHelperInput{}, err
	}
	procedure, err := OpenNetworkWorkInput(attempt, request.Assignment.Input)
	if err != nil {
		return NetworkHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return NetworkHelperInput{}, err
	}
	attempt.StartedAt = &started
	reported, err := workexecution.DecodeAgentAttempt(request.Report.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, reported) {
		return NetworkHelperInput{}, invalidNetworkWork()
	}
	progress, err := networkexecution.DecodeProgress(attempt, procedure.Action, procedure.NetworkId, request.Report.Progress)
	if err != nil {
		return NetworkHelperInput{}, err
	}
	if !progress.InFlight {
		return NetworkHelperInput{}, invalidNetworkWork()
	}
	ack := request.Acknowledgement
	acknowledged, err := workexecution.DecodeAgentAttempt(ack.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, acknowledged) || ack.Evidence == nil ||
		ack.RecordedAt == nil || ack.RecordedAt.CheckValid() != nil {
		return NetworkHelperInput{}, invalidNetworkWork()
	}
	checkpoint, err := progress.Checkpoint(attempt, procedure.Action, procedure.NetworkId)
	if err != nil {
		return NetworkHelperInput{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if checkpoint.Sequence != ack.Sequence || checkpoint.StageID != ack.StageId || checkpoint.Evidence != evidence ||
		!checkpoint.RecordedAt.Equal(ack.RecordedAt.AsTime()) {
		return NetworkHelperInput{}, invalidNetworkWork()
	}

	return NetworkHelperInput{Attempt: attempt, Procedure: procedure, Progress: progress, InspectOnly: request.InspectOnly}, nil
}

func ReadNetworkHelperObservation(input NetworkHelperInput, response *agentpb.NetworkWorkHelperResponse) (networkexecution.Observation, error) {
	if input.Procedure == nil || response == nil || RejectUnknown(response) != nil || response.Input == nil ||
		response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil || response.Inspection != input.InspectOnly {
		return networkexecution.Observation{}, invalidNetworkWork()
	}
	observation := networkexecution.Observation{
		NetworkID: response.NetworkId,
		Input:     workexecution.Input{ID: response.Input.InputId, SHA256: hex.EncodeToString(response.Input.Sha256)},
		Ready:     response.Ready, Absent: response.Absent, At: response.ObservedAt.AsTime().UTC(),
	}
	if input.InspectOnly {
		if input.Progress.Validate(input.Attempt, input.Procedure.Action, input.Procedure.NetworkId) != nil ||
			!input.Progress.InFlight || observation.NetworkID != input.Procedure.NetworkId || observation.Input != input.Attempt.Input ||
			observation.At.Before(*input.Progress.BeganAt) || observation.Ready && observation.Absent {
			return networkexecution.Observation{}, invalidNetworkWork()
		}
		return observation, nil
	}
	if _, err := input.Progress.Confirm(input.Attempt, input.Procedure.Action, input.Procedure.NetworkId, observation, observation.At); err != nil {
		return networkexecution.Observation{}, err
	}
	return observation, nil
}
