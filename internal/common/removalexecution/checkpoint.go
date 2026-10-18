package removalexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaximumProgressBytes = 4096

func (progress Progress) StageID(attempt workexecution.Attempt) (string, error) {
	if attempt.Input.Validate() != nil || progress.Sequence == 0 {
		return "", invalid()
	}
	phase := progress.Phase
	if !progress.InFlight {
		switch phase {
		case InspectRuntime:
			phase = QuiesceExecutors
		case RestoreConfiguration:
			phase = InspectRuntime
		case RestoreRuntime:
			phase = RestoreConfiguration
		case VerifyRestoration:
			phase = RestoreRuntime
		case Complete:
			phase = VerifyRestoration
		default:
			return "", invalid()
		}
	}
	if _, valid := phaseIndex(phase); !valid || phase == Complete {
		return "", invalid()
	}
	at, err := ids.Timestamp(ids.KindPlan, attempt.Input.ID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindStep, at, attempt.Input.ID, "service-removal/recovery", string(phase)), nil
}

func (progress Progress) Checkpoint(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, capture RecoveryCapture) (workexecution.Checkpoint, error) {
	if progress.Validate(attempt, input, capture) != nil {
		return workexecution.Checkpoint{}, invalid()
	}
	return progress.CheckpointEnvelope(attempt, capture.Source.Forward.ID, capture.Source.Evidence)
}

// CheckpointEnvelope binds the metadata-only Agent journal to its captured
// forward result. It does not authorize execution without the full assignment.
func (progress Progress) CheckpointEnvelope(attempt workexecution.Attempt, forwardID string, failure workexecution.Input) (workexecution.Checkpoint, error) {
	if progress.ValidateEnvelope(attempt) != nil || ids.Validate(ids.KindAttempt, forwardID) != nil || failure.Validate() != nil {
		return workexecution.Checkpoint{}, invalid()
	}
	stage, err := progress.StageID(attempt)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	// Terminal metadata cannot change an already acknowledged report's hash.
	attempt.FinishedAt, attempt.Result, attempt.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt   workexecution.Attempt `json:"attempt"`
		ForwardID string                `json:"forward_id"`
		Failure   workexecution.Input   `json:"failure"`
		Progress  Progress              `json:"progress"`
	}{attempt, forwardID, failure, progress})
	if err != nil {
		return workexecution.Checkpoint{}, invalid()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(raw)
	return workexecution.Checkpoint{AttemptID: attempt.ID, StageID: stage, Sequence: progress.Sequence,
		RecordedAt: progress.UpdatedAt, Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EncodeProgress(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, capture RecoveryCapture, progress Progress) ([]byte, error) {
	if _, err := progress.Checkpoint(attempt, input, capture); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(progress)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, invalid()
	}
	return jcs.Canonicalize(raw)
}

func DecodeProgress(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, capture RecoveryCapture, raw []byte) (Progress, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return Progress{}, invalid()
	}
	progress, err := jcs.Decode[Progress](raw)
	if err != nil {
		return Progress{}, invalid()
	}
	expected, err := EncodeProgress(attempt, input, capture, progress)
	if err != nil || !bytes.Equal(expected, raw) {
		return Progress{}, invalid()
	}
	return progress, nil
}
