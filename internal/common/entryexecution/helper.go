package entryexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// HelperInput validates captured authority, not authentication. The launcher
// still owns the host lease and authenticates the Controller channel.
type HelperInput struct {
	FileInput
	InspectOnly bool
}

func OpenHelperRequest(request *agentpb.EntryConfigurationWorkHelperRequest) (HelperInput, error) {
	if request == nil || proto.Size(request) > executionplan.MaximumPlanBytes || executionplan.RejectUnknown(request) != nil ||
		request.Assignment == nil || request.Assignment.Attempt == nil {
		return HelperInput{}, invalidLayout()
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId,
		AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return HelperInput{}, invalidLayout()
	}
	procedure, err := executionplan.OpenEntryConfigurationWorkInput(attempt, request.Assignment.Input)
	if err != nil {
		return HelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return HelperInput{}, err
	}
	attempt.StartedAt = &started
	if attempt.Direction == workexecution.Recovery {
		if request.Report != nil || request.Acknowledgement != nil {
			return HelperInput{}, invalidLayout()
		}
		source, err := DecodeRecovery(request.Assignment.RecoverySource, procedure)
		if err != nil || source.ValidateFor(attempt, procedure) != nil {
			return HelperInput{}, invalidLayout()
		}
		boundary, err := DecodeProgress(source.Forward, procedure, request.Assignment.RecoveryBoundary)
		if err != nil {
			return HelperInput{}, err
		}
		checkpoint, err := boundary.Checkpoint(source.Forward, procedure)
		if err != nil || source.Failure.Checkpoint == nil || checkpoint != *source.Failure.Checkpoint {
			return HelperInput{}, invalidLayout()
		}
		return HelperInput{FileInput: FileInput{Attempt: attempt, Procedure: procedure, Progress: boundary, Recovery: &source}, InspectOnly: request.InspectOnly}, nil
	}
	if attempt.Direction != workexecution.Forward || request.Report == nil || request.Acknowledgement == nil ||
		len(request.Assignment.RecoverySource) != 0 || len(request.Assignment.RecoveryBoundary) != 0 {
		return HelperInput{}, invalidLayout()
	}
	reported, err := workexecution.DecodeAgentAttempt(request.Report.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, reported) {
		return HelperInput{}, invalidLayout()
	}
	progress, err := DecodeProgress(attempt, procedure, request.Report.Progress)
	if err != nil || !progress.InFlight ||
		(progress.Stage != ReconcileConsumers && progress.Stage != VerifyConsumers) ||
		progress.Stage == VerifyConsumers && !request.InspectOnly {
		return HelperInput{}, invalidLayout()
	}
	ack := request.Acknowledgement
	acknowledged, err := workexecution.DecodeAgentAttempt(ack.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, acknowledged) || ack.Evidence == nil ||
		ack.RecordedAt == nil || ack.RecordedAt.CheckValid() != nil {
		return HelperInput{}, invalidLayout()
	}
	checkpoint, err := progress.Checkpoint(attempt, procedure)
	if err != nil {
		return HelperInput{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if checkpoint.Sequence != ack.Sequence || checkpoint.StageID != ack.StageId || checkpoint.Evidence != evidence ||
		!checkpoint.RecordedAt.Equal(ack.RecordedAt.AsTime()) {
		return HelperInput{}, invalidLayout()
	}
	return HelperInput{FileInput: FileInput{Attempt: attempt, Procedure: procedure, Progress: progress},
		InspectOnly: request.InspectOnly}, nil
}
