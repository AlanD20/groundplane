package releaseexecution

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// FailureReceipt acknowledges recording the failure, not restoring its effects.
type FailureReceipt struct {
	Evidence  workexecution.Input   `json:"evidence"`
	Failure   Failure               `json:"failure"`
	Outcome   workexecution.Outcome `json:"outcome"`
	StartedAt *time.Time            `json:"started_at,omitempty"`
}

func (receipt FailureReceipt) Validate(attempt workexecution.Attempt, reason FailureReason) error {
	if receipt.Outcome != workexecution.Failed && receipt.Outcome != workexecution.Cancelled && receipt.Outcome != workexecution.Superseded || receipt.Failure.Reason != reason {
		return invalidProgress()
	}
	if attempt.StartedAt != nil && (receipt.StartedAt == nil || !attempt.StartedAt.Equal(*receipt.StartedAt)) {
		return invalidProgress()
	}
	attempt.StartedAt = receipt.StartedAt
	_, reference, err := EncodeFailure(attempt, receipt.Failure)
	if err != nil || reference != receipt.Evidence {
		return invalidProgress()
	}
	return nil
}

func DecodeFailureAck(attempt workexecution.Attempt, reason FailureReason, ack *agentpb.ReleaseWorkFailureAck) (FailureReceipt, error) {
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return FailureReceipt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || !proto.Equal(identity, ack.Attempt) || ack.Evidence == nil ||
		ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil {
		return FailureReceipt{}, invalidProgress()
	}
	receipt := FailureReceipt{Evidence: workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}, Outcome: workexecution.Outcome(ack.Outcome)}
	if ack.StartedAt != nil {
		if ack.StartedAt.CheckValid() != nil {
			return FailureReceipt{}, invalidProgress()
		}
		started := ack.StartedAt.AsTime()
		receipt.StartedAt = &started
	}
	if attempt.StartedAt != nil && (receipt.StartedAt == nil || !attempt.StartedAt.Equal(*receipt.StartedAt)) {
		return FailureReceipt{}, invalidProgress()
	}
	attempt.StartedAt = receipt.StartedAt
	receipt.Failure, err = DecodeFailure(attempt, ack.Failure, receipt.Evidence)
	if err != nil || !receipt.Failure.RecordedAt.Equal(ack.FinishedAt.AsTime()) || receipt.Validate(attempt, reason) != nil {
		return FailureReceipt{}, invalidProgress()
	}
	return receipt, nil
}
