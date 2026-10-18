package componentexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type FailureReason string

const (
	ExecutionFailed      FailureReason = "execution_failed"
	DeadlineExpired      FailureReason = "deadline_expired"
	ExecutionInterrupted FailureReason = "execution_interrupted"
)

func (reason FailureReason) Valid() bool {
	return reason == ExecutionFailed || reason == DeadlineExpired || reason == ExecutionInterrupted
}

// Failure preserves the Controller's last acknowledged boundary.
// It records no private command output and makes no claim that effects stopped.
type Failure struct {
	AttemptID  string                    `json:"attempt_id"`
	Input      workexecution.Input       `json:"input"`
	Reason     FailureReason             `json:"reason"`
	Checkpoint *workexecution.Checkpoint `json:"checkpoint,omitempty"`
	RecordedAt time.Time                 `json:"recorded_at"`
}

func FailureID(attemptID string) (string, error) {
	at, err := ids.Timestamp(ids.KindAttempt, attemptID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "component/failure"), nil
}

func EncodeFailure(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, failure Failure) ([]byte, workexecution.Input, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return nil, workexecution.Input{}, invalidProgress()
	}
	return EncodeLayoutFailure(attempt, layout, failure)
}

func EncodeLayoutFailure(attempt workexecution.Attempt, layout Layout, failure Failure) ([]byte, workexecution.Input, error) {
	if layout.Validate(attempt) != nil {
		return nil, workexecution.Input{}, invalidProgress()
	}
	if attempt.Validate() != nil || failure.AttemptID != attempt.ID ||
		attempt.Direction == workexecution.Recovery && failure.Checkpoint != nil ||
		failure.Input != attempt.Input || !failure.Reason.Valid() || failure.RecordedAt.IsZero() || failure.RecordedAt.Location() != time.UTC ||
		failure.RecordedAt.Before(attempt.AssignedAt) || attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		failure.Reason == DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return nil, workexecution.Input{}, invalidProgress()
	}
	if checkpoint := failure.Checkpoint; checkpoint != nil {
		if attempt.StartedAt == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Sequence < 1 || checkpoint.Sequence > uint64(2*len(layout.stages())) ||
			ids.Validate(ids.KindStep, checkpoint.StageID) != nil || checkpoint.Evidence.Validate() != nil ||
			checkpoint.RecordedAt.Before(*attempt.StartedAt) || checkpoint.RecordedAt.After(failure.RecordedAt) {
			return nil, workexecution.Input{}, invalidProgress()
		}
		stageAt, err := ids.Timestamp(ids.KindWorkItem, attempt.WorkItemID)
		if err != nil {
			return nil, workexecution.Input{}, err
		}
		stage := layout.stages()[(checkpoint.Sequence-1)/2]
		if checkpoint.StageID != ids.DeriveAt(ids.KindStep, stageAt, attempt.WorkItemID, "component/"+string(stage)) ||
			checkpoint.Evidence.ID != attempt.Input.ID {
			return nil, workexecution.Input{}, invalidProgress()
		}
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return nil, workexecution.Input{}, invalidProgress()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	id, err := FailureID(attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeFailure(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, raw []byte, reference workexecution.Input) (Failure, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return Failure{}, err
	}
	return DecodeLayoutFailure(attempt, layout, raw, reference)
}

func DecodeLayoutFailure(attempt workexecution.Attempt, layout Layout, raw []byte, reference workexecution.Input) (Failure, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Failure{}, invalidProgress()
	}
	failure, err := jcs.Decode[Failure](raw)
	if err != nil {
		return Failure{}, invalidProgress()
	}
	expected, evidence, err := EncodeLayoutFailure(attempt, layout, failure)
	if err != nil || !bytes.Equal(expected, raw) || evidence != reference {
		return Failure{}, invalidProgress()
	}
	return failure, nil
}
