package executionplan

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
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupRecoveryReport records observed cleanup, not a replacement outcome for
// the original failure. The Controller also checks resource checkpoints before
// accepting release. Prune retains remote deletion obligations independently.
type BackupRecoveryReport struct {
	AttemptID       string                      `json:"attempt_id"`
	ForwardID       string                      `json:"forward_id"`
	Failure         workexecution.Input         `json:"failure"`
	ExecutorStopped bool                        `json:"executor_stopped"`
	Operation       workexecution.Operation     `json:"operation"`
	Sources         []BackupSourceRecoveryProof `json:"sources,omitempty"`
	ObservedAt      time.Time                   `json:"observed_at"`
}

// Each proof names one captured source. These observations are meaningful only
// together with Controller-owned mutation and consumer recovery checkpoints.
type BackupSourceRecoveryProof struct {
	Ordinal       uint32 `json:"ordinal"`
	PointID       string `json:"point_id"`
	StagingAbsent bool   `json:"staging_absent"`
	HelperRetired bool   `json:"helper_retired"`
}

const MaximumBackupRecoveryReportBytes = 64 * 1024

func (report BackupRecoveryReport) ValidateInput(input *agentpb.BackupWorkInput) error {
	if err := ValidateBackupWorkInput(input); err != nil {
		return err
	}
	var expected workexecution.Operation
	switch input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		expected = workexecution.Backup
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		expected = workexecution.Restore
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		expected = workexecution.BackupPrune
	}
	if report.Operation != expected {
		return errs.New(errs.KindStateConflict, "Backup recovery operation differs from captured input")
	}
	if expected == workexecution.BackupPrune {
		return nil
	}
	if len(report.Sources) != len(input.Steps) {
		return errs.New(errs.KindStateConflict, "Backup recovery must account for every captured source")
	}
	for ordinal, step := range input.Steps {
		point := step.GetCapture().GetPointId()
		if expected == workexecution.Restore {
			point = step.GetRestore().GetPointId()
		}
		if report.Sources[ordinal].Ordinal != uint32(ordinal) || report.Sources[ordinal].PointID != point {
			return errs.New(errs.KindStateConflict, "Backup recovery proof names a different source")
		}
	}
	return nil
}

func (report BackupRecoveryReport) Validate(attempt workexecution.Attempt, source BackupWorkRecovery) error {
	if source.ValidateFor(attempt) != nil || attempt.StartedAt == nil || report.AttemptID != attempt.ID ||
		report.ForwardID != source.Forward.ID || report.Failure != source.Evidence || !report.ExecutorStopped ||
		report.ObservedAt.IsZero() || report.ObservedAt.Location() != time.UTC ||
		report.ObservedAt.Before(*attempt.StartedAt) || report.ObservedAt.Before(source.Failure.RecordedAt) {
		return errs.New(errs.KindValidationFailed, "Backup prune recovery does not prove exclusion of the captured executor")
	}
	switch report.Operation {
	case workexecution.BackupPrune:
		if len(report.Sources) != 0 {
			return errs.New(errs.KindValidationFailed, "Prune recovery cannot claim source cleanup")
		}
	case workexecution.Backup, workexecution.Restore:
		if len(report.Sources) == 0 || len(report.Sources) > MaximumBackupSources ||
			report.Operation == workexecution.Restore && len(report.Sources) != 1 {
			return errs.New(errs.KindValidationFailed, "Backup recovery source evidence is incomplete")
		}
		for ordinal, proof := range report.Sources {
			if proof.Ordinal != uint32(ordinal) || proof.PointID == "" || len(proof.PointID) > 128 ||
				!proof.StagingAbsent || !proof.HelperRetired {
				return errs.New(errs.KindValidationFailed, "Backup recovery source is not settled")
			}
		}
	default:
		return errs.New(errs.KindValidationFailed, "Backup recovery operation is invalid")
	}
	return nil
}

func EncodeBackupRecoveryReport(attempt workexecution.Attempt, source BackupWorkRecovery,
	report BackupRecoveryReport,
) ([]byte, workexecution.Input, error) {
	if err := report.Validate(attempt, source); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(raw) > MaximumBackupRecoveryReportBytes {
		return nil, workexecution.Input{}, errs.New(errs.KindValidationFailed, "Backup recovery report is oversized")
	}
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, at, attempt.ID, "backup/recovery"), SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeBackupRecoveryReport(attempt workexecution.Attempt, source BackupWorkRecovery,
	raw []byte,
) (BackupRecoveryReport, workexecution.Input, error) {
	if len(raw) == 0 || len(raw) > MaximumBackupRecoveryReportBytes {
		return BackupRecoveryReport{}, workexecution.Input{}, errs.New(errs.KindValidationFailed, "Backup prune recovery report is missing or oversized")
	}
	report, err := jcs.Decode[BackupRecoveryReport](raw)
	if err != nil {
		return BackupRecoveryReport{}, workexecution.Input{}, err
	}
	canonical, evidence, err := EncodeBackupRecoveryReport(attempt, source, report)
	if err != nil {
		return BackupRecoveryReport{}, workexecution.Input{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return BackupRecoveryReport{}, workexecution.Input{}, errs.New(errs.KindValidationFailed, "Backup prune recovery report is not canonical")
	}
	return report, evidence, nil
}
