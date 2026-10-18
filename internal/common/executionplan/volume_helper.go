package executionplan

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// VolumeHelperInput is opened only after all captured content and receipts
// agree. The helper transport/launcher authenticates who supplied this envelope;
// the self-described Agent identity is not an authentication credential.
type VolumeHelperInput struct {
	Attempt        workexecution.Attempt
	Procedure      *agentpb.VolumeWorkInput
	Progress       volumeexecution.Progress
	InspectOnly    bool
	DirectoryBatch *volumeexecution.DirectoryBatch
	Recovery       *volumeexecution.Recovery
}

func OpenVolumeHelperRequest(request *agentpb.VolumeWorkHelperRequest) (VolumeHelperInput, error) {
	if request != nil && request.Assignment != nil && len(request.Assignment.RecoverySource) != 0 {
		return openVolumeRecoveryHelper(request)
	}
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil ||
		request.Assignment == nil || request.Assignment.Attempt == nil || request.Report == nil || request.Acknowledgement == nil {
		return VolumeHelperInput{}, invalidVolumeWork()
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId,
		AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	procedure, err := OpenVolumeWorkInput(attempt, request.Assignment.Input)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	attempt.StartedAt = &started
	reported, err := workexecution.DecodeAgentAttempt(request.Report.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, reported) {
		return VolumeHelperInput{}, invalidVolumeWork()
	}
	progress, err := volumeexecution.DecodeProgress(attempt, procedure.Action, procedure.VolumeId, request.Report.Progress)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	if !progress.InFlight {
		return VolumeHelperInput{}, invalidVolumeWork()
	}
	ack := request.Acknowledgement
	acknowledged, err := workexecution.DecodeAgentAttempt(ack.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, acknowledged) || ack.Evidence == nil ||
		ack.RecordedAt == nil || ack.RecordedAt.CheckValid() != nil {
		return VolumeHelperInput{}, invalidVolumeWork()
	}
	checkpoint, err := progress.Checkpoint(attempt, procedure.Action, procedure.VolumeId)
	if err != nil {
		return VolumeHelperInput{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if checkpoint.Sequence != ack.Sequence || checkpoint.StageID != ack.StageId || checkpoint.Evidence != evidence ||
		!checkpoint.RecordedAt.Equal(ack.RecordedAt.AsTime()) {
		return VolumeHelperInput{}, invalidVolumeWork()
	}
	input := VolumeHelperInput{Attempt: attempt, Procedure: procedure, Progress: progress, InspectOnly: request.InspectOnly}
	needsBatch := progress.Stage == volumeexecution.RemoveDirectory && !request.InspectOnly
	if needsBatch != (len(request.DirectoryBatch) != 0) {
		return VolumeHelperInput{}, invalidVolumeWork()
	}
	if needsBatch {
		batch, err := volumeexecution.DecodeDirectoryBatch(attempt, progress, request.DirectoryBatch)
		if err != nil || batch.VolumeID != procedure.VolumeId {
			return VolumeHelperInput{}, invalidVolumeWork()
		}
		input.DirectoryBatch = &batch
	}
	return input, nil
}

func ValidateVolumeHelperResponse(input VolumeHelperInput, response *agentpb.VolumeWorkHelperResponse) error {
	if input.DirectoryBatch == nil {
		if response == nil || len(response.DirectoryResult) != 0 {
			return invalidVolumeWork()
		}
		_, err := ReadVolumeHelperObservation(input, response)
		return err
	}
	if input.Procedure == nil || response == nil || RejectUnknown(response) != nil || response.Input == nil ||
		response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil || response.Inspection || input.InspectOnly ||
		response.VolumeId != input.Procedure.VolumeId || !response.ConsumersDetached || !response.VolumeAbsent || response.VolumeReady ||
		response.DirectoryReady && response.DirectoryAbsent {
		return invalidVolumeWork()
	}
	evidence := workexecution.Input{ID: response.Input.InputId, SHA256: hex.EncodeToString(response.Input.Sha256)}
	if evidence != input.Attempt.Input {
		return invalidVolumeWork()
	}
	result, err := volumeexecution.DecodeDirectoryResult(input.Attempt, input.Progress, *input.DirectoryBatch, response.DirectoryResult)
	if err != nil || result.Absent != response.DirectoryAbsent || !result.ObservedAt.Equal(response.ObservedAt.AsTime()) {
		return invalidVolumeWork()
	}
	return nil
}

func ReadVolumeHelperObservation(input VolumeHelperInput, response *agentpb.VolumeWorkHelperResponse) (volumeexecution.Observation, error) {
	if input.Procedure == nil || response == nil || RejectUnknown(response) != nil || response.Input == nil ||
		response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil || response.Inspection != input.InspectOnly {
		return volumeexecution.Observation{}, invalidVolumeWork()
	}
	observation := volumeexecution.Observation{
		VolumeID: response.VolumeId, Input: workexecution.Input{ID: response.Input.InputId, SHA256: hex.EncodeToString(response.Input.Sha256)},
		DirectoryReady: response.DirectoryReady, VolumeReady: response.VolumeReady, ConsumersDetached: response.ConsumersDetached,
		VolumeAbsent: response.VolumeAbsent, DirectoryAbsent: response.DirectoryAbsent, At: response.ObservedAt.AsTime().UTC(),
	}
	if input.Recovery != nil {
		report := volumeexecution.RecoveryReport{AttemptID: input.Attempt.ID, ForwardID: input.Recovery.Forward.ID,
			Failure: input.Recovery.Evidence, HelperStopped: true, Observation: observation}
		if err := report.Validate(input.Attempt, *input.Recovery, input.Procedure.Action, input.Procedure.VolumeId, input.Progress); err != nil {
			return volumeexecution.Observation{}, err
		}
		return observation, nil
	}
	if input.InspectOnly {
		if input.Progress.Validate(input.Attempt, input.Procedure.Action, input.Procedure.VolumeId) != nil ||
			!input.Progress.InFlight || observation.VolumeID != input.Procedure.VolumeId || observation.Input != input.Attempt.Input ||
			observation.At.Before(*input.Progress.BeganAt) || observation.DirectoryReady && observation.DirectoryAbsent ||
			observation.VolumeReady && observation.VolumeAbsent {
			return volumeexecution.Observation{}, invalidVolumeWork()
		}
		return observation, nil
	}
	if _, err := input.Progress.Confirm(input.Attempt, input.Procedure.Action, input.Procedure.VolumeId, observation, observation.At); err != nil {
		return volumeexecution.Observation{}, err
	}
	return observation, nil
}
