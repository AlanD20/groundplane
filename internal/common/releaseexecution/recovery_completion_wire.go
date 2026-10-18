package releaseexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Recovery uses the shared completion transport with its actual direction and
// checkpoint. It never manufactures a successful forward execution.
func EncodeRecoveryCompletionRequest(input RecoveryInput, report RecoveryReport) (*agentpb.ReleaseWorkCompletionRequest, error) {
	if report.Ack == nil || report.Validate(input) != nil ||
		report.Progress.Phase != RecoveryComplete || report.Progress.InFlight {
		return nil, invalidProgress()
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(report.Ack.Evidence.SHA256)
	if err != nil {
		return nil, invalidProgress()
	}
	return &agentpb.ReleaseWorkCompletionRequest{Attempt: attempt, CheckpointSequence: report.Progress.Sequence,
		CheckpointEvidence: &agentpb.WorkInputReference{InputId: report.Ack.Evidence.ID, Sha256: digest}}, nil
}

func DecodeRecoveryCompletionAck(input RecoveryInput, report RecoveryReport, ack *agentpb.ReleaseWorkCompletionAck) (CompletionReceipt, error) {
	request, err := EncodeRecoveryCompletionRequest(input, report)
	if err != nil {
		return CompletionReceipt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || !proto.Equal(ack.Attempt, request.Attempt) ||
		ack.CheckpointSequence != request.CheckpointSequence || !proto.Equal(ack.CheckpointEvidence, request.CheckpointEvidence) ||
		ack.Outcome != string(workexecution.Succeeded) || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil ||
		ack.FinishedAt.AsTime().Before(report.Progress.UpdatedAt) {
		return CompletionReceipt{}, invalidProgress()
	}
	return CompletionReceipt{Outcome: workexecution.Succeeded, FinishedAt: ack.FinishedAt.AsTime()}, nil
}
