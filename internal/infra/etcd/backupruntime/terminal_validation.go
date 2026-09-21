package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateBackupTerminalReceiptRecord(record BackupTerminalReceiptRecord) error {
	if record.PriorTaskRevision <= 0 || record.PriorEnvironmentEpochRevision <= 0 ||
		!recordcodec.ValidSHA256(record.EnvironmentEpochDigest) ||
		!recordcodec.ValidSHA256(record.DomainDigest) ||
		!recordcodec.ValidSHA256(record.ReceiptDigest) || ValidateBackupTerminalTaskEvidence(record.Task) != nil {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt identity is invalid")
	}
	epochDigest, err := BackupTerminalEnvironmentEpochDigest(record.Task.Owner.EnvironmentID)
	if err != nil || epochDigest != record.EnvironmentEpochDigest {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Environment epoch is invalid")
	}
	switch record.Task.TaskType {
	case taskjournal.TaskBackup:
		if len(record.Sources) == 0 || len(record.Sources) > backuppolicy.MaximumBackupPolicySources ||
			len(record.Points) != 0 {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt sources are invalid")
		}
		for index, source := range record.Sources {
			if source.Ordinal != uint32(index) ||
				recordcodec.ValidateID(ids.KindBackupSource, source.SourceID) != nil ||
				source.TargetID == "" ||
				recordcodec.ValidateID(ids.KindRecoveryPoint, source.RecoveryPointID) != nil ||
				!ValidBackupRuntimeInstant(source.RecoveryPointCreatedAt) ||
				source.RecoveryPointCreatedAt.After(record.Task.FinishedAt) ||
				!ValidBackupSourceAttemptState(source.State) ||
				!ValidBackupSourceAttemptPhase(source.Phase) ||
				!ValidBackupFailureCodeForAttempt(source.State, source.Phase, source.FailureCode) ||
				source.SizeBytes < 0 || (source.SHA256 != "" && !recordcodec.ValidSHA256(source.SHA256)) ||
				((source.SizeBytes > 0) != (source.SHA256 != "")) {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt source is invalid")
			}
			switch source.Kind {
			case BackupRuntimeSourceAttach, BackupRuntimeSourceVolume, BackupRuntimeSourceConfig:
			default:
				return errs.New(errs.KindValidationFailed, "backup terminal receipt source kind is invalid")
			}
		}
	case taskjournal.TaskBackupPrune:
		if len(record.Sources) != 0 || len(record.Points) == 0 ||
			len(record.Points) > MaximumBackupPruneDispatchPoints {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt points are invalid")
		}
		seen := make(map[string]struct{}, len(record.Points))
		for _, point := range record.Points {
			if ValidateBackupRecoveryPointSnapshot(point.Point) != nil ||
				point.Point.EnvironmentID != record.Task.Owner.EnvironmentID ||
				!ValidBackupRuntimeInstant(point.CreatedAt) ||
				point.CreatedAt.After(record.Task.FinishedAt) {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt point is invalid")
			}
			if _, exists := seen[point.Point.ID]; exists {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt points are duplicated")
			}
			seen[point.Point.ID] = struct{}{}
			if point.Outcome != BackupPruneTerminalRemoved &&
				point.Outcome != BackupPruneTerminalRetained {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt outcome is invalid")
			}
		}
		domainDigest, err := BackupTerminalDomainDigest(record.Points)
		if err != nil || domainDigest != record.DomainDigest {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt point digest is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task type is invalid")
	}
	receiptDigest, err := BackupTerminalReceiptDigest(record)
	if err != nil {
		return err
	}
	if receiptDigest != record.ReceiptDigest {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt digest is invalid")
	}
	return nil
}

func ValidateBackupTerminalTaskEvidence(evidence BackupTerminalTaskEvidence) error {
	if recordcodec.ValidateID(ids.KindTask, evidence.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, evidence.OperationID) != nil ||
		(evidence.RetryOf != "" && recordcodec.ValidateID(ids.KindTask, evidence.RetryOf) != nil) ||
		taskjournal.ValidateOwner(evidence.Owner) != nil || evidence.Owner.EnvironmentID == "" ||
		!taskjournal.ValidActor(evidence.Actor) || !taskjournal.ValidExecutor(evidence.Executor) ||
		evidence.Executor != taskjournal.TaskExecutorAgent || evidence.Target != evidence.Owner.EnvironmentID ||
		recordcodec.ValidateID(ids.KindPlan, evidence.PlanID) != nil || !recordcodec.ValidSHA256(evidence.PlanHash) ||
		!taskjournal.IsTerminalTaskStatus(evidence.Status) || !recordcodec.ValidSHA256(evidence.ResultDigest) ||
		!recordcodec.ValidSHA256(evidence.TaskDigest) || recordcodec.ValidateTimestamp("receipt created_at", evidence.CreatedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt updated_at", evidence.UpdatedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt finished_at", evidence.FinishedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt retain_until", evidence.RetainUntil) != nil ||
		!evidence.UpdatedAt.Equal(evidence.FinishedAt) ||
		!evidence.RetainUntil.Equal(evidence.FinishedAt.Add(taskjournal.TaskRetention)) {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task evidence is invalid")
	}
	if evidence.StartedAt != nil && recordcodec.ValidateTimestamp("receipt started_at", *evidence.StartedAt) != nil {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task start is invalid")
	}
	if evidence.TerminalAssignment != nil {
		assignment := evidence.TerminalAssignment
		if evidence.StartedAt == nil || recordcodec.ValidateID(ids.KindAssignment, assignment.AssignmentID) != nil ||
			recordcodec.ValidateID(ids.KindAgent, assignment.AgentID) != nil || assignment.AgentGeneration == 0 {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt assignment is invalid")
		}
	}
	if evidence.TaskType != taskjournal.TaskBackup && evidence.TaskType != taskjournal.TaskBackupPrune {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task evidence type is invalid")
	}
	if evidence.TaskType == taskjournal.TaskBackupPrune && evidence.Actor != taskjournal.TaskActorSystem {
		return errs.New(errs.KindValidationFailed, "backup prune terminal receipt actor is invalid")
	}
	return nil
}
