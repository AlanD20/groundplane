package scriptexecutions

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScriptRecoverySettlement is separate from forward checkpoints and outcome.
// Stopping an interrupted hook must never invent its exit code or erase its
// original failure. Evidence identifies the owning Release recovery proof.
type ScriptRecoverySettlement struct {
	AttemptID  string                       `json:"attempt_id"`
	Evidence   workexecution.Input          `json:"evidence"`
	Cleanup    scriptexecution.CleanupProof `json:"cleanup"`
	ObservedAt time.Time                    `json:"observed_at"`
	SettledAt  time.Time                    `json:"settled_at"`
}

func RecoveryProofKey(executionID, attemptID string) string {
	return ScriptExecutionKey(executionID) + "/recoveries/" + attemptID
}

func validateRecoverySettlement(record ScriptExecutionRecord) error {
	settled := record.Recovery
	if settled == nil {
		return nil
	}
	cleanup := settled.Cleanup
	if record.WorkInput == nil || ids.Validate(ids.KindAttempt, settled.AttemptID) != nil ||
		settled.AttemptID == record.AttemptID || settled.Evidence.Validate() != nil ||
		settled.ObservedAt.IsZero() || settled.ObservedAt.Location() != time.UTC ||
		settled.SettledAt.Location() != time.UTC || settled.SettledAt.Before(settled.ObservedAt) ||
		settled.ObservedAt.Before(record.CreatedAt) || record.UpdatedAt.Before(settled.SettledAt) ||
		!cleanup.ContainerAbsent || !cleanup.BodyAbsent || !cleanup.ExecutionDirectoryAbsent {
		return errs.New(errs.KindValidationFailed, "Script recovery settlement is invalid")
	}
	if record.ContainerCreated == nil && cleanup.ContainerID != "" ||
		record.ContainerCreated != nil && cleanup.ContainerID != record.ContainerCreated.ContainerID ||
		record.BodyPrepared == nil && (cleanup.BodyDevice != 0 || cleanup.BodyInode != 0 || cleanup.BodyLeaf != "") ||
		record.BodyPrepared != nil && (cleanup.BodyDevice != record.BodyPrepared.Device ||
			cleanup.BodyInode != record.BodyPrepared.Inode || cleanup.BodyLeaf != record.BodyPrepared.Leaf) {
		return errs.New(errs.KindValidationFailed, "Script recovery settlement differs from captured artifact receipts")
	}
	return nil
}

func SettleRecovery(record ScriptExecutionRecord, settlement ScriptRecoverySettlement) (ScriptExecutionRecord, error) {
	if ValidateScriptExecutionRecord(record) != nil || record.Recovery != nil ||
		settlement.SettledAt.Before(record.UpdatedAt) {
		return ScriptExecutionRecord{}, errs.New(
			errs.KindStateConflict,
			"Script recovery cannot settle changed or already settled execution",
		)
	}
	record.Recovery = &settlement
	record.UpdatedAt = settlement.SettledAt
	if err := ValidateScriptExecutionRecord(record); err != nil {
		return ScriptExecutionRecord{}, err
	}
	return record, nil
}
