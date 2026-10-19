package releaseexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type RecoveryEffectResult struct {
	Quiescence    *QuiescenceProof            `json:"quiescence,omitempty"`
	Configuration *RecoveryConfigurationProof `json:"configuration,omitempty"`
	Runtime       *RecoveryRuntimeProof       `json:"runtime,omitempty"`
	Hook          *FailureHookProof           `json:"hook,omitempty"`
}

func (result RecoveryEffectResult) Validate(input RecoveryInput, boundary RecoveryReport) error {
	if boundary.Validate(input) != nil || boundary.Ack == nil || !boundary.Progress.InFlight {
		return invalidProgress()
	}
	if boundary.Progress.Phase == FailureHooks {
		if result.Hook == nil || result.Quiescence != nil || result.Configuration != nil || result.Runtime != nil {
			return invalidProgress()
		}
		return result.Hook.Validate(input, boundary)
	}
	if result.Hook != nil {
		return invalidProgress()
	}
	if input.PreservesServing() && result.Runtime != nil && !result.Runtime.Inspection {
		return invalidProgress()
	}
	switch boundary.Progress.Phase {
	case QuiesceForward:
		if result.Quiescence == nil || result.Configuration != nil || result.Runtime != nil {
			return invalidProgress()
		}
	case RestoreConfiguration:
		if result.Quiescence != nil || result.Configuration == nil || result.Runtime != nil {
			return invalidProgress()
		}
	case InspectFailedRuntime, RestoreServingRuntime, RemoveFailedCandidate:
		if result.Quiescence != nil || result.Configuration != nil || result.Runtime == nil {
			return invalidProgress()
		}
	case VerifyRestoration:
		// Runtime alone cannot prove that the captured files were restored.
		if result.Quiescence != nil || result.Configuration == nil || result.Runtime == nil ||
			!result.Runtime.Inspection {
			return invalidProgress()
		}
	default:
		return invalidProgress()
	}
	if result.Quiescence != nil && result.Quiescence.Validate(input, boundary) != nil ||
		result.Configuration != nil && result.Configuration.Validate(input) != nil ||
		result.Runtime != nil && result.Runtime.Validate(input, boundary) != nil {
		return invalidProgress()
	}
	return nil
}

type RecoveryEffectEvidence struct {
	AttemptID  string               `json:"attempt_id"`
	Input      workexecution.Input  `json:"input"`
	Sequence   uint64               `json:"sequence"`
	StageID    string               `json:"stage_id"`
	RecordedAt time.Time            `json:"recorded_at"`
	Result     RecoveryEffectResult `json:"result"`
}

func EncodeRecoveryEffect(
	input RecoveryInput,
	boundary RecoveryReport,
	result RecoveryEffectResult,
	at time.Time,
) ([]byte, workexecution.Input, error) {
	if result.Validate(input, boundary) != nil || at.IsZero() || at.Before(boundary.Progress.UpdatedAt) ||
		!at.Before(input.Input.Attempt.Deadline) ||
		result.Quiescence != nil && at.Before(result.Quiescence.OwnerCheckedAt) ||
		result.Runtime != nil && at.Before(result.Runtime.ObservedAt) {
		return nil, workexecution.Input{}, invalidProgress()
	}
	evidence := RecoveryEffectEvidence{AttemptID: input.Input.Attempt.ID, Input: input.Input.Attempt.Input,
		Sequence: boundary.Progress.Sequence, StageID: boundary.StageID, RecordedAt: at.UTC(), Result: result}
	raw, err := canonicalCheckpoint(evidence)
	if err != nil || len(raw) > MaximumCheckpointBytes {
		return nil, workexecution.Input{}, invalidProgress()
	}
	reference, err := recoveryEffectReference(input.Input.Attempt, boundary, raw)
	return raw, reference, err
}

// DecodeRecoveryEffectEnvelope validates compact journal identity without a
// duplicate copy of runtime artifacts. Consumers of proof must use the full
// DecodeRecoveryEffect before authorizing or confirming an action.
func DecodeRecoveryEffectEnvelope(
	input RecoveryInput,
	boundary RecoveryReport,
	raw []byte,
	reference workexecution.Input,
) (RecoveryEffectEvidence, error) {
	if input.ValidateIdentity() != nil || boundary.ValidateEnvelope(input) != nil || boundary.Ack == nil ||
		!boundary.Progress.InFlight ||
		len(raw) == 0 ||
		len(raw) > MaximumCheckpointBytes {
		return RecoveryEffectEvidence{}, invalidProgress()
	}
	evidence, err := jcs.Decode[RecoveryEffectEvidence](raw)
	if err != nil || evidence.AttemptID != input.Input.Attempt.ID || evidence.Input != input.Input.Attempt.Input ||
		evidence.Sequence != boundary.Progress.Sequence || evidence.StageID != boundary.StageID || evidence.RecordedAt.IsZero() ||
		evidence.RecordedAt.Location() != time.UTC || evidence.RecordedAt.Before(boundary.Progress.UpdatedAt) || !evidence.RecordedAt.Before(input.Input.Attempt.Deadline) {
		return RecoveryEffectEvidence{}, invalidProgress()
	}
	canonical, err := canonicalCheckpoint(evidence)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RecoveryEffectEvidence{}, invalidProgress()
	}
	expected, err := recoveryEffectReference(input.Input.Attempt, boundary, raw)
	if err != nil || expected != reference {
		return RecoveryEffectEvidence{}, invalidProgress()
	}
	return evidence, nil
}

func DecodeRecoveryEffect(
	input RecoveryInput,
	boundary RecoveryReport,
	raw []byte,
	reference workexecution.Input,
) (RecoveryEffectEvidence, error) {
	evidence, err := DecodeRecoveryEffectEnvelope(input, boundary, raw, reference)
	if err != nil {
		return RecoveryEffectEvidence{}, err
	}
	canonical, expected, err := EncodeRecoveryEffect(input, boundary, evidence.Result, evidence.RecordedAt)
	if err != nil || expected != reference || !bytes.Equal(canonical, raw) {
		return RecoveryEffectEvidence{}, invalidProgress()
	}
	return evidence, nil
}

func recoveryEffectReference(
	attempt workexecution.Attempt,
	boundary RecoveryReport,
	raw []byte,
) (workexecution.Input, error) {
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return workexecution.Input{}, err
	}
	id := ids.DeriveAt(
		ids.KindPlan,
		at,
		attempt.ID,
		fmt.Sprintf("release/recovery-effect/%d/%s", boundary.Progress.Sequence, boundary.StageID),
	)
	digest := sha256.Sum256(raw)
	return workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}, nil
}
