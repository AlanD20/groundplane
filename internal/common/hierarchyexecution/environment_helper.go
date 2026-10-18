package hierarchyexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func OpenEnvironmentHelperRequest(request *agentpb.EnvironmentRemovalHelperRequest) (EnvironmentInput, EnvironmentProgress, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || request.Assignment == nil || request.Assignment.Attempt == nil {
		return EnvironmentInput{}, EnvironmentProgress{}, invalidEnvironmentProgress()
	}
	claimed := request.Assignment.Attempt
	input, err := DecodeEnvironmentAssignment(request.Assignment, workexecution.Identity{
		Executor: workexecution.Agent, HostID: claimed.HostId, AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration,
	})
	if err != nil {
		return EnvironmentInput{}, EnvironmentProgress{}, err
	}
	input, _, err = input.WithStart(request.Start)
	if err != nil {
		return EnvironmentInput{}, EnvironmentProgress{}, err
	}
	report, err := DecodeEnvironmentCheckpointRequest(input, request.Report)
	if err != nil {
		return EnvironmentInput{}, EnvironmentProgress{}, err
	}
	if ValidateEnvironmentCheckpointAck(input, report, request.Acknowledgement) != nil || !report.Progress.InFlight {
		return EnvironmentInput{}, EnvironmentProgress{}, invalidEnvironmentProgress()
	}
	return input, report.Progress, nil
}

func DecodeEnvironmentHelperResponse(input EnvironmentInput, progress EnvironmentProgress, inspectOnly bool,
	response *agentpb.EnvironmentRemovalHelperResponse,
) (EnvironmentObservation, error) {
	if response == nil || executionplan.RejectUnknown(response) != nil || response.CapturedSource == nil ||
		response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil || response.Inspection != inspectOnly {
		return EnvironmentObservation{}, invalidEnvironmentProgress()
	}
	observation := EnvironmentObservation{EnvironmentID: response.EnvironmentId,
		Source:        workexecution.Input{ID: response.CapturedSource.InputId, SHA256: hex.EncodeToString(response.CapturedSource.Sha256)},
		RuntimeAbsent: response.RuntimeAbsent, DirectoryAbsent: response.DirectoryAbsent, At: response.ObservedAt.AsTime(),
	}
	if _, err := progress.Confirm(input, observation, observation.At); err != nil {
		return EnvironmentObservation{}, err
	}
	return observation, nil
}
