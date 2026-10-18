package releaseexecution

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// QuiescenceProof records the authenticated Agent's process exclusion and helper
// absence observation. It does not attest workload state or restoration; those
// belong to the later recovery phases.
type QuiescenceProof struct {
	Hooks             []HookQuiescenceProof  `json:"hooks"`
	ForwardAttemptID  string                 `json:"forward_attempt_id"`
	RecoveryAttemptID string                 `json:"recovery_attempt_id"`
	Executor          workexecution.Identity `json:"executor"`
	HelperAbsentAt    time.Time              `json:"helper_absent_at"`
	OwnerCheckedAt    time.Time              `json:"owner_checked_at"`
}

type HookQuiescenceProof struct {
	Cleanup     scriptexecution.CleanupProof `json:"cleanup"`
	ExecutionID string                       `json:"execution_id"`
	Input       workexecution.Input          `json:"input"`
	ContainerID string                       `json:"container_id,omitempty"`
	Absent      bool                         `json:"absent"`
	ObservedAt  time.Time                    `json:"observed_at"`
}

func (proof QuiescenceProof) Validate(input RecoveryInput, report RecoveryReport) error {
	if report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight || report.Progress.Phase != QuiesceForward ||
		proof.ForwardAttemptID != input.Forward.ID || proof.RecoveryAttemptID != input.Input.Attempt.ID ||
		proof.Executor != input.Input.Attempt.Executor || proof.HelperAbsentAt.IsZero() || proof.OwnerCheckedAt.IsZero() ||
		proof.HelperAbsentAt.Location() != time.UTC || proof.OwnerCheckedAt.Location() != time.UTC ||
		proof.HelperAbsentAt.Before(report.Progress.UpdatedAt) || proof.OwnerCheckedAt.Before(proof.HelperAbsentAt) ||
		!proof.OwnerCheckedAt.Before(input.Input.Attempt.Deadline) {
		return invalidProgress()
	}
	if len(proof.Hooks) != len(input.Input.Release.Hooks) {
		return invalidProgress()
	}
	for index, hook := range input.Input.Release.Hooks {
		observed := proof.Hooks[index]
		if observed.ExecutionID != hook.ExecutionId || observed.Input.ID != hook.Input.InputId ||
			!observed.Cleanup.ContainerAbsent || !observed.Cleanup.BodyAbsent || !observed.Cleanup.ExecutionDirectoryAbsent ||
			observed.Cleanup.ContainerID != "" && observed.ContainerID != "" && observed.Cleanup.ContainerID != observed.ContainerID ||
			observed.Input.SHA256 != hex.EncodeToString(hook.Input.Sha256) ||
			observed.ObservedAt.IsZero() || observed.ObservedAt.Location() != time.UTC ||
			observed.ObservedAt.Before(report.Progress.UpdatedAt) || observed.ObservedAt.After(proof.OwnerCheckedAt) ||
			!observed.Absent {
			return invalidProgress()
		}
		if (observed.Cleanup.BodyDevice == 0 || observed.Cleanup.BodyInode == 0 || observed.Cleanup.BodyLeaf != "body") &&
			(observed.Cleanup.BodyDevice != 0 || observed.Cleanup.BodyInode != 0 || observed.Cleanup.BodyLeaf != "") {
			return invalidProgress()
		}
		if observed.ContainerID != "" {
			decoded, err := hex.DecodeString(observed.ContainerID)
			if err != nil || len(decoded) != 32 {
				return invalidProgress()
			}
		}
		if observed.Cleanup.ContainerID != "" {
			decoded, err := hex.DecodeString(observed.Cleanup.ContainerID)
			if err != nil || len(decoded) != 32 {
				return invalidProgress()
			}
		}
	}
	return nil
}
