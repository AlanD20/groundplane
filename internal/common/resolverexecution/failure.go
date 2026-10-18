package resolverexecution

import (
	"bytes"
	"time"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type FailureReason string

const (
	ExecutionFailed FailureReason = "execution_failed"
	Interrupted     FailureReason = "execution_interrupted"
	DeadlineExpired FailureReason = "deadline_expired"
)

// Failure contains execution authority, not raw Docker, DNS or hook output.
// Original failure and the outcome of cleanup remain independent records.
type Failure struct {
	AttemptID  string                    `json:"attempt_id"`
	Input      workexecution.Input       `json:"input"`
	Reason     FailureReason             `json:"reason"`
	Checkpoint *workexecution.Checkpoint `json:"checkpoint,omitempty"`
	RecordedAt time.Time                 `json:"recorded_at"`
}

func (failure Failure) Validate(attempt workexecution.Attempt, layout Layout) error {
	if layout.Validate(attempt) != nil || attempt.Direction != workexecution.Forward || failure.AttemptID != attempt.ID ||
		failure.Input != attempt.Input || !canonicalTime(failure.RecordedAt) || failure.RecordedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		attempt.FinishedAt != nil && !attempt.FinishedAt.Equal(failure.RecordedAt) ||
		failure.Reason != ExecutionFailed && failure.Reason != Interrupted && failure.Reason != DeadlineExpired ||
		failure.Reason == DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return invalid("DNS resolver failure differs from its captured forward attempt")
	}
	if cursor := failure.Checkpoint; cursor != nil {
		if attempt.StartedAt == nil || cursor.AttemptID != attempt.ID || cursor.Sequence == 0 || cursor.Sequence > uint64(len(layout.Stages())*2) ||
			cursor.Evidence.Validate() != nil || cursor.Evidence.ID != attempt.Input.ID || !canonicalTime(cursor.RecordedAt) ||
			cursor.RecordedAt.Before(*attempt.StartedAt) || cursor.RecordedAt.After(failure.RecordedAt) {
			return invalid("DNS resolver failure checkpoint is invalid")
		}
		stageID, err := StageID(attempt.WorkItemID, layout.Stages()[(cursor.Sequence-1)/2])
		if err != nil || stageID != cursor.StageID {
			return invalid("DNS resolver failure names a different execution stage")
		}
	}
	return nil
}

func EncodeFailure(attempt workexecution.Attempt, layout Layout, failure Failure) ([]byte, workexecution.Input, error) {
	if err := failure.Validate(attempt, layout); err != nil {
		return nil, workexecution.Input{}, err
	}
	id, err := FailureID(attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	return captureEvidence(id, failure)
}

func FailureID(attemptID string) (string, error) { return derivedReferenceID(attemptID, "failure") }

func DecodeFailure(attempt workexecution.Attempt, layout Layout, raw []byte, evidence workexecution.Input) (Failure, error) {
	if len(raw) == 0 || len(raw) > MaximumReportBytes {
		return Failure{}, invalid("DNS resolver failure is missing or oversized")
	}
	failure, err := jcs.Decode[Failure](raw)
	if err != nil {
		return Failure{}, invalid("DNS resolver failure cannot be decoded")
	}
	canonical, reference, err := EncodeFailure(attempt, layout, failure)
	if err != nil || reference != evidence || !bytes.Equal(canonical, raw) {
		return Failure{}, invalid("DNS resolver failure differs from its saved evidence")
	}
	return failure, nil
}
