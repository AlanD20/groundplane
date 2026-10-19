package releaseexecution

import (
	"bytes"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryRuntimeProof is emitted only after the selected host operation has
// established its result. Runtime verification does not substitute for file
// verification, nor does this report settle the original failed action.
type RecoveryRuntimeProof struct {
	AttemptID  string             `json:"attempt_id"`
	StageID    string             `json:"stage_id"`
	Phase      RecoveryPhase      `json:"phase"`
	Inspection bool               `json:"inspection"`
	ObservedAt time.Time          `json:"observed_at"`
	Inventory  *RecoveryInventory `json:"inventory,omitempty"`
}

func (proof RecoveryRuntimeProof) Validate(input RecoveryInput, report RecoveryReport) error {
	if report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight ||
		proof.AttemptID != input.Input.Attempt.ID || proof.StageID != report.StageID || proof.Phase != report.Progress.Phase ||
		proof.ObservedAt.IsZero() || proof.ObservedAt.Location() != time.UTC || proof.ObservedAt.Before(report.Progress.UpdatedAt) ||
		!proof.ObservedAt.Before(input.Input.Attempt.Deadline) {
		return invalidProgress()
	}
	switch proof.Phase {
	case InspectFailedRuntime:
		if proof.Inventory == nil || proof.Inventory.Validate(input, report) != nil ||
			proof.ObservedAt.Before(proof.Inventory.ObservedAt) {
			return invalidProgress()
		}
	case RestoreServingRuntime, RemoveFailedCandidate, VerifyRestoration:
		if proof.Inventory != nil {
			return invalidProgress()
		}
	default:
		return invalidProgress()
	}
	return nil
}

func EncodeRecoveryRuntimeProof(
	input RecoveryInput,
	report RecoveryReport,
	proof RecoveryRuntimeProof,
) (*agentpb.ReleaseRecoveryHelperResponse, error) {
	if err := proof.Validate(input, report); err != nil {
		return nil, err
	}
	raw, err := canonicalCheckpoint(proof)
	if err != nil || len(raw) > MaximumCheckpointBytes {
		return nil, invalidProgress()
	}
	return &agentpb.ReleaseRecoveryHelperResponse{Proof: raw}, nil
}

func DecodeRecoveryRuntimeProof(
	input RecoveryInput,
	report RecoveryReport,
	response *agentpb.ReleaseRecoveryHelperResponse,
) (RecoveryRuntimeProof, error) {
	if response == nil || executionplan.RejectUnknown(response) != nil || len(response.Proof) == 0 ||
		len(response.Proof) > MaximumCheckpointBytes {
		return RecoveryRuntimeProof{}, invalidProgress()
	}
	proof, err := jcs.Decode[RecoveryRuntimeProof](response.Proof)
	if err != nil || proof.Validate(input, report) != nil {
		return RecoveryRuntimeProof{}, invalidProgress()
	}
	raw, err := canonicalCheckpoint(proof)
	if err != nil || !bytes.Equal(raw, response.Proof) {
		return RecoveryRuntimeProof{}, invalidProgress()
	}
	return proof, nil
}
