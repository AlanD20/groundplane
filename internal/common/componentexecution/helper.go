package componentexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// HelperInput is validated authority, not authentication. The helper launcher
// must own the host lease and authenticate the transport supplying this input.
type HelperInput struct {
	Attempt     workexecution.Attempt
	Procedure   *agentpb.ComponentWorkInput
	Progress    Progress
	InspectOnly bool
	Recovery    *Recovery
}

func OpenHelperRequest(request *agentpb.ComponentWorkHelperRequest) (HelperInput, error) {
	if request == nil || proto.Size(request) > executionplan.MaximumPlanBytes ||
		executionplan.RejectUnknown(request) != nil ||
		request.Assignment == nil ||
		request.Assignment.Attempt == nil {
		return HelperInput{}, invalidProgress()
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId,
		AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return HelperInput{}, err
	}
	procedure, err := executionplan.OpenComponentWorkInput(attempt, request.Assignment.Input)
	if err != nil {
		return HelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return HelperInput{}, err
	}
	attempt.StartedAt = &started
	result := HelperInput{Attempt: attempt, Procedure: procedure, InspectOnly: request.InspectOnly}
	if attempt.Direction == workexecution.Recovery {
		if request.Report != nil || request.Acknowledgement != nil || request.InspectOnly {
			return HelperInput{}, invalidProgress()
		}
		source, err := DecodeRecovery(request.Assignment.RecoverySource, procedure)
		if err != nil || source.ValidateFor(attempt, procedure) != nil {
			return HelperInput{}, invalidProgress()
		}
		boundary, err := DecodeProgress(source.Forward, procedure, request.Assignment.RecoveryBoundary)
		if err != nil {
			return HelperInput{}, err
		}
		checkpoint, err := boundary.Checkpoint(source.Forward, procedure)
		if err != nil || source.Failure.Checkpoint == nil || checkpoint != *source.Failure.Checkpoint {
			return HelperInput{}, invalidProgress()
		}
		result.Recovery, result.Progress = &source, boundary
		return result, nil
	}
	if len(request.Assignment.RecoverySource) != 0 || len(request.Assignment.RecoveryBoundary) != 0 ||
		request.Report == nil ||
		request.Acknowledgement == nil {
		return HelperInput{}, invalidProgress()
	}
	reported, err := workexecution.DecodeAgentAttempt(request.Report.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, reported) {
		return HelperInput{}, invalidProgress()
	}
	progress, err := DecodeProgress(attempt, procedure, request.Report.Progress)
	if err != nil || !progress.InFlight {
		return HelperInput{}, invalidProgress()
	}
	ack := request.Acknowledgement
	acknowledged, err := workexecution.DecodeAgentAttempt(ack.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, acknowledged) || ack.Evidence == nil ||
		ack.RecordedAt == nil ||
		ack.RecordedAt.CheckValid() != nil {
		return HelperInput{}, invalidProgress()
	}
	checkpoint, err := progress.Checkpoint(attempt, procedure)
	if err != nil {
		return HelperInput{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if checkpoint.Sequence != ack.Sequence || checkpoint.StageID != ack.StageId || checkpoint.Evidence != evidence ||
		!checkpoint.RecordedAt.Equal(ack.RecordedAt.AsTime()) {
		return HelperInput{}, invalidProgress()
	}
	result.Progress = progress
	return result, nil
}
