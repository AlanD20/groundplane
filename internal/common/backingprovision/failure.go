package backingprovision

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
	ExecutionFailed FailureReason = "execution_failed"
	DeadlineExpired FailureReason = "deadline_expired"
	Interrupted     FailureReason = "execution_interrupted"
)

// Failure is supplied by the Controller from the last durable checkpoint.
// Process output and secret values do not belong in this recovery authority.
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
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "backing-creation/failure"), nil
}

func EncodeFailure(attempt workexecution.Attempt, layout Layout, failure Failure) ([]byte, workexecution.Input, error) {
	if layout.Validate(attempt) != nil || attempt.Direction != workexecution.Forward ||
		failure.AttemptID != attempt.ID ||
		failure.Input != attempt.Input ||
		failure.Reason != ExecutionFailed && failure.Reason != DeadlineExpired && failure.Reason != Interrupted ||
		failure.RecordedAt.IsZero() ||
		failure.RecordedAt.Location() != time.UTC ||
		failure.RecordedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		failure.Reason == DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return nil, workexecution.Input{}, invalid("backing creation failure does not match its forward attempt")
	}
	if cursor := failure.Checkpoint; cursor != nil {
		if attempt.StartedAt == nil || cursor.AttemptID != attempt.ID || cursor.Sequence == 0 ||
			cursor.Sequence > uint64(len(layout.stages())*2) ||
			cursor.Evidence.Validate() != nil ||
			cursor.Evidence.ID != attempt.Input.ID ||
			cursor.RecordedAt.IsZero() ||
			cursor.RecordedAt.Location() != time.UTC ||
			cursor.RecordedAt.Before(*attempt.StartedAt) ||
			cursor.RecordedAt.After(failure.RecordedAt) {
			return nil, workexecution.Input{}, invalid("backing creation failure has an invalid checkpoint")
		}
		stageID, err := StageID(attempt.WorkItemID, layout.stages()[(cursor.Sequence-1)/2])
		if err != nil || stageID != cursor.StageID {
			return nil, workexecution.Input{}, invalid("backing creation failure checkpoint names a different stage")
		}
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return nil, workexecution.Input{}, invalid("backing creation failure cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	id, err := FailureID(attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(canonical)
	return canonical, workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeFailure(
	attempt workexecution.Attempt,
	layout Layout,
	raw []byte,
	evidence workexecution.Input,
) (Failure, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Failure{}, invalid("backing creation failure receipt is missing or oversized")
	}
	failure, err := jcs.Decode[Failure](raw)
	if err != nil {
		return Failure{}, invalid("backing creation failure receipt cannot be decoded")
	}
	canonical, expected, err := EncodeFailure(attempt, layout, failure)
	if err != nil || expected != evidence || !bytes.Equal(canonical, raw) {
		return Failure{}, invalid("backing creation failure receipt differs from its captured evidence")
	}
	return failure, nil
}
