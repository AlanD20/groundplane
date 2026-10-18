package removalexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Failure describes only this cleanup attempt. OriginalFailure binds it to the
// retained forward failure; neither cleanup timeout nor its success replaces it.
type Failure struct {
	AttemptID       string                           `json:"attempt_id"`
	OriginalFailure workexecution.Input              `json:"original_failure"`
	Reason          lifecycleexecution.FailureReason `json:"reason"`
	Progress        *Progress                        `json:"progress,omitempty"`
	RecordedAt      time.Time                        `json:"recorded_at"`
}

func (failure Failure) Validate(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, capture RecoveryCapture) error {
	if err := capture.ValidateFor(attempt, input); err != nil {
		return err
	}
	if failure.OriginalFailure != capture.Source.Evidence {
		return invalid()
	}
	return failure.ValidateEnvelope(attempt)
}

func (failure Failure) ValidateEnvelope(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery || failure.AttemptID != attempt.ID ||
		failure.OriginalFailure.Validate() != nil || !failure.Reason.Valid() ||
		failure.RecordedAt.IsZero() || failure.RecordedAt.Location() != time.UTC || failure.RecordedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		attempt.FinishedAt != nil && !failure.RecordedAt.Equal(*attempt.FinishedAt) ||
		failure.Reason == lifecycleexecution.DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return invalid()
	}
	if progress := failure.Progress; progress != nil {
		if progress.ValidateEnvelope(attempt) != nil || progress.Sequence == 0 ||
			progress.Phase == Complete || progress.UpdatedAt.After(failure.RecordedAt) {
			return invalid()
		}
	}
	return nil
}

func EncodeFailure(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, capture RecoveryCapture, failure Failure) ([]byte, workexecution.Input, error) {
	if err := failure.Validate(attempt, input, capture); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(failure)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, workexecution.Input{}, invalid()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, at, attempt.ID, "service-removal/recovery-failure"),
		SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeFailure(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, capture RecoveryCapture, raw []byte, reference workexecution.Input) (Failure, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Failure{}, invalid()
	}
	failure, err := jcs.Decode[Failure](raw)
	if err != nil {
		return Failure{}, invalid()
	}
	expected, evidence, err := EncodeFailure(attempt, input, capture, failure)
	if err != nil || !bytes.Equal(expected, raw) || evidence != reference {
		return Failure{}, invalid()
	}
	return failure, nil
}
