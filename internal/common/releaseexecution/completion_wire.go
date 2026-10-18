package releaseexecution

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type CompletionReceipt struct {
	Outcome    workexecution.Outcome `json:"outcome"`
	FinishedAt time.Time             `json:"finished_at"`
}

func EncodeCompletionRequest(input Input, report CheckpointReport) (*agentpb.ReleaseWorkCompletionRequest, error) {
	if report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil ||
		report.Progress.Phase != Complete || report.Progress.InFlight || input.Attempt.Direction != workexecution.Forward {
		return nil, invalidProgress()
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(report.Ack.Evidence.SHA256)
	if err != nil {
		return nil, invalidProgress()
	}
	return &agentpb.ReleaseWorkCompletionRequest{Attempt: attempt, CheckpointSequence: report.Sequence,
		CheckpointEvidence: &agentpb.WorkInputReference{InputId: report.Ack.Evidence.ID, Sha256: digest}}, nil
}

func DecodeCompletionAck(input Input, report CheckpointReport, ack *agentpb.ReleaseWorkCompletionAck) (CompletionReceipt, error) {
	request, err := EncodeCompletionRequest(input, report)
	if err != nil {
		return CompletionReceipt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || !proto.Equal(ack.Attempt, request.Attempt) ||
		ack.CheckpointSequence != request.CheckpointSequence || !proto.Equal(ack.CheckpointEvidence, request.CheckpointEvidence) ||
		ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil || ack.FinishedAt.AsTime().Before(report.Progress.UpdatedAt) {
		return CompletionReceipt{}, invalidProgress()
	}
	outcome := workexecution.Outcome(ack.Outcome)
	if outcome != workexecution.Succeeded && outcome != workexecution.Cancelled && outcome != workexecution.Superseded {
		return CompletionReceipt{}, invalidProgress()
	}
	return CompletionReceipt{Outcome: outcome, FinishedAt: ack.FinishedAt.AsTime()}, nil
}
