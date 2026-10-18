package lifecycleexecution

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

// Failure retains the last acknowledged boundary. It does not assert that an
// in-flight container operation or hook stopped, and contains no command output.
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
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "service-lifecycle/failure"), nil
}

func EncodeFailure(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, failure Failure) ([]byte, workexecution.Input, error) {
	stages, err := Stages(input)
	if err != nil ||
		input.CapturedSource == nil || input.CapturedSource.InputId != attempt.Input.ID ||
		hex.EncodeToString(input.CapturedSource.Sha256) != attempt.Input.SHA256 {
		return nil, workexecution.Input{}, invalid()
	}
	return EncodeFailureStages(attempt, stages, failure)
}

func EncodeFailureStages(attempt workexecution.Attempt, stages []Stage, failure Failure) ([]byte, workexecution.Input, error) {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || validateStages(stages) != nil ||
		failure.AttemptID != attempt.ID || failure.Input != attempt.Input || !failure.Reason.Valid() ||
		failure.RecordedAt.IsZero() || failure.RecordedAt.Location() != time.UTC || failure.RecordedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && failure.RecordedAt.Before(*attempt.StartedAt) ||
		failure.Reason == DeadlineExpired && failure.RecordedAt.Before(attempt.Deadline) {
		return nil, workexecution.Input{}, invalid()
	}
	if checkpoint := failure.Checkpoint; checkpoint != nil {
		knownStage := false
		for _, stage := range stages {
			knownStage = knownStage || stage.ID == checkpoint.StageID
		}
		if attempt.StartedAt == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Sequence == 0 ||
			checkpoint.Sequence > uint64(2*len(stages)) || !knownStage ||
			checkpoint.Evidence.Validate() != nil || checkpoint.Evidence.ID != attempt.Input.ID ||
			checkpoint.RecordedAt.Location() != time.UTC || checkpoint.RecordedAt.Before(*attempt.StartedAt) || checkpoint.RecordedAt.After(failure.RecordedAt) {
			return nil, workexecution.Input{}, invalid()
		}
	}
	raw, err := json.Marshal(failure)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, workexecution.Input{}, invalid()
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

func DecodeFailure(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, raw []byte, reference workexecution.Input) (Failure, error) {
	stages, err := Stages(input)
	if err != nil || input.CapturedSource == nil || input.CapturedSource.InputId != attempt.Input.ID || hex.EncodeToString(input.CapturedSource.Sha256) != attempt.Input.SHA256 {
		return Failure{}, invalid()
	}
	return DecodeFailureStages(attempt, stages, raw, reference)
}

func DecodeFailureStages(attempt workexecution.Attempt, stages []Stage, raw []byte, reference workexecution.Input) (Failure, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Failure{}, invalid()
	}
	failure, err := jcs.Decode[Failure](raw)
	if err != nil {
		return Failure{}, invalid()
	}
	expected, evidence, err := EncodeFailureStages(attempt, stages, failure)
	if err != nil || !bytes.Equal(expected, raw) || evidence != reference {
		return Failure{}, invalid()
	}
	return failure, nil
}
