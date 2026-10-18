package releaseexecution

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

// Failure records an attempt failure independently of other attempts. Recovery
// failures use their own attempt identity and never replace the forward failure.
// It carries no subprocess output, configuration values or private error text.
// The Controller supplies the saved checkpoint, not the reporting Agent.
type Failure struct {
	AttemptID  string                    `json:"attempt_id"`
	Input      workexecution.Input       `json:"input"`
	Reason     FailureReason             `json:"reason"`
	Checkpoint *workexecution.Checkpoint `json:"checkpoint,omitempty"`
	RecordedAt time.Time                 `json:"recorded_at"`
}

func EncodeFailure(attempt workexecution.Attempt, failure Failure) ([]byte, workexecution.Input, error) {
	if attempt.Validate() != nil || failure.AttemptID != attempt.ID || failure.Input != attempt.Input ||
		(failure.Reason != ExecutionFailed && failure.Reason != DeadlineExpired && failure.Reason != ExecutionInterrupted) ||
		failure.RecordedAt.IsZero() || failure.RecordedAt.Location() != time.UTC || failure.RecordedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		failure.Reason == DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return nil, workexecution.Input{}, invalidProgress()
	}
	if checkpoint := failure.Checkpoint; checkpoint != nil {
		if attempt.StartedAt == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Sequence == 0 ||
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
	id, err := FailureEvidenceID(attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	reference := workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}
	return raw, reference, nil
}

// FailureEvidenceID identifies one attempt-owned failure record, including
// for retention. Its derivation cannot depend on the latest work outcome.
func FailureEvidenceID(attemptID string) (string, error) {
	at, err := ids.Timestamp(ids.KindAttempt, attemptID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "release/failure"), nil
}

func DecodeFailure(attempt workexecution.Attempt, raw []byte, reference workexecution.Input) (Failure, error) {
	if len(raw) == 0 || len(raw) > MaximumCheckpointBytes {
		return Failure{}, invalidProgress()
	}
	failure, err := jcs.Decode[Failure](raw)
	if err != nil {
		return Failure{}, invalidProgress()
	}
	canonical, expected, err := EncodeFailure(attempt, failure)
	if err != nil || !bytes.Equal(canonical, raw) || expected != reference {
		return Failure{}, invalidProgress()
	}
	return failure, nil
}
