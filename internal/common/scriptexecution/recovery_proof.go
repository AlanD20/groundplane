package scriptexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoveryProof records absence, not a successful shell exit. The original
// forward outcome remains unchanged when this proof settles cleanup.
type RecoveryProof struct {
	AttemptID        string              `json:"attempt_id"`
	ForwardAttemptID string              `json:"forward_attempt_id"`
	ExecutionID      string              `json:"execution_id"`
	Input            workexecution.Input `json:"input"`
	Quiescence       QuiescenceProof     `json:"quiescence"`
	Cleanup          CleanupProof        `json:"cleanup"`
	ObservedAt       time.Time           `json:"observed_at"`
}

func (proof RecoveryProof) Validate(input RecoveryInput) error {
	if input.Validate() != nil || input.Attempt.StartedAt == nil || proof.AttemptID != input.Attempt.ID ||
		proof.ForwardAttemptID != input.Forward.ID || proof.ExecutionID != input.ExecutionID || proof.Input != input.Attempt.Input ||
		!proof.Quiescence.Absent || !proof.Cleanup.ContainerAbsent || !proof.Cleanup.BodyAbsent || !proof.Cleanup.ExecutionDirectoryAbsent ||
		proof.Quiescence.ObservedAt.IsZero() || proof.Quiescence.ObservedAt.Location() != time.UTC ||
		proof.Quiescence.ObservedAt.Before(*input.Attempt.StartedAt) || proof.ObservedAt.Location() != time.UTC ||
		proof.ObservedAt.Before(proof.Quiescence.ObservedAt) || !proof.ObservedAt.Before(input.Attempt.Deadline) {
		return errs.New(errs.KindStateConflict, "Script recovery proof does not establish cleanup of this attempt")
	}
	if container := input.Artifacts.ContainerCreated; container != nil {
		if proof.Cleanup.ContainerID != container.ContainerId ||
			proof.Quiescence.ContainerID != "" && proof.Quiescence.ContainerID != container.ContainerId {
			return errs.New(errs.KindStateConflict, "Script recovery proof identifies a different container")
		}
	} else if proof.Cleanup.ContainerID != "" || proof.Quiescence.ContainerID != "" {
		return errs.New(errs.KindStateConflict, "Script recovery proof has no captured container receipt")
	}
	if body := input.Artifacts.BodyPrepared; body != nil {
		if proof.Cleanup.BodyDevice != body.Device || proof.Cleanup.BodyInode != body.Inode ||
			proof.Cleanup.BodyLeaf != body.Leaf {
			return errs.New(errs.KindStateConflict, "Script recovery proof identifies a different body file")
		}
	} else if proof.Cleanup.BodyDevice != 0 || proof.Cleanup.BodyInode != 0 || proof.Cleanup.BodyLeaf != "" {
		return errs.New(errs.KindStateConflict, "Script recovery proof has no captured body receipt")
	}
	return nil
}

func EncodeRecoveryProof(input RecoveryInput, proof RecoveryProof) ([]byte, workexecution.Input, error) {
	if err := proof.Validate(input); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(proof)
	if err != nil {
		return nil, workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(raw) > 16<<10 {
		return nil, workexecution.Input{}, errs.New(
			errs.KindValidationFailed,
			"Script cleanup proof exceeds its size limit",
		)
	}
	digest := sha256.Sum256(raw)
	reference := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, input.Attempt.AssignedAt, input.Attempt.ID, "script-cleanup"),
		SHA256: hex.EncodeToString(digest[:]),
	}
	return raw, reference, nil
}

func DecodeRecoveryProof(input RecoveryInput, raw []byte, reference workexecution.Input) (RecoveryProof, error) {
	if len(raw) == 0 || len(raw) > 16<<10 {
		return RecoveryProof{}, errs.New(errs.KindValidationFailed, "Script cleanup proof size is invalid")
	}
	proof, err := jcs.Decode[RecoveryProof](raw)
	if err != nil {
		return RecoveryProof{}, err
	}
	canonical, expected, err := EncodeRecoveryProof(input, proof)
	if err != nil {
		return RecoveryProof{}, err
	}
	if expected != reference || !bytes.Equal(raw, canonical) {
		return RecoveryProof{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup proof differs from its captured evidence",
		)
	}
	return proof, nil
}
