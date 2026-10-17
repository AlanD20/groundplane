package volumeexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
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
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "volume/failure"), nil
}

func EncodeFailure(attempt workexecution.Attempt, failure Failure) ([]byte, workexecution.Input, error) {
	if attempt.Validate() != nil || failure.AttemptID != attempt.ID ||
		failure.Input != attempt.Input || !failure.Reason.Valid() || failure.RecordedAt.IsZero() || failure.RecordedAt.Location() != time.UTC ||
		failure.RecordedAt.Before(attempt.AssignedAt) || attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		failure.Reason == DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return nil, workexecution.Input{}, invalidProgress()
	}
	if checkpoint := failure.Checkpoint; checkpoint != nil {
		if attempt.StartedAt == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Sequence < 1 || checkpoint.Sequence > 6 ||
			ids.Validate(ids.KindStep, checkpoint.StageID) != nil || checkpoint.Evidence.Validate() != nil ||
			checkpoint.RecordedAt.Before(*attempt.StartedAt) || checkpoint.RecordedAt.After(failure.RecordedAt) {
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

func DecodeFailure(attempt workexecution.Attempt, raw []byte, reference workexecution.Input) (Failure, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Failure{}, invalidProgress()
	}
	failure, err := jcs.Decode[Failure](raw)
	if err != nil {
		return Failure{}, invalidProgress()
	}
	expected, evidence, err := EncodeFailure(attempt, failure)
	if err != nil || !bytes.Equal(expected, raw) || evidence != reference {
		return Failure{}, invalidProgress()
	}
	return failure, nil
}
