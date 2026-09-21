package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateBackupTerminalReceiptRecord(record BackupTerminalReceiptRecord) error {
	if record.PriorTaskRevision <= 0 || record.PriorEnvironmentEpochRevision <= 0 ||
		!recordcodec.ValidSHA256(record.EnvironmentEpochDigest) ||
		!recordcodec.ValidSHA256(record.DomainDigest) ||
		!recordcodec.ValidSHA256(record.ReceiptDigest) || validateBackupTerminalTaskEvidence(record.Task) != nil {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt identity is invalid")
	}
	epochDigest, err := backupTerminalEnvironmentEpochDigest(record.Task.Owner.EnvironmentID)
	if err != nil || epochDigest != record.EnvironmentEpochDigest {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Environment epoch is invalid")
	}
	switch record.Task.TaskType {
	case TaskBackup:
		if len(record.Sources) == 0 || len(record.Sources) > backuppolicy.MaximumBackupPolicySources ||
			len(record.Points) != 0 {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt sources are invalid")
		}
		for index, source := range record.Sources {
			if source.Ordinal != uint32(index) ||
				recordcodec.ValidateID(ids.KindBackupSource, source.SourceID) != nil ||
				source.TargetID == "" ||
				recordcodec.ValidateID(ids.KindRecoveryPoint, source.RecoveryPointID) != nil ||
				!backupruntime.ValidBackupRuntimeInstant(source.RecoveryPointCreatedAt) ||
				source.RecoveryPointCreatedAt.After(record.Task.FinishedAt) ||
				!backupruntime.ValidBackupSourceAttemptState(source.State) ||
				!backupruntime.ValidBackupSourceAttemptPhase(source.Phase) ||
				!backupruntime.ValidBackupFailureCodeForAttempt(source.State, source.Phase, source.FailureCode) ||
				source.SizeBytes < 0 || (source.SHA256 != "" && !recordcodec.ValidSHA256(source.SHA256)) ||
				((source.SizeBytes > 0) != (source.SHA256 != "")) {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt source is invalid")
			}
			switch source.Kind {
			case backupruntime.BackupRuntimeSourceAttach, backupruntime.BackupRuntimeSourceVolume, BackupRuntimeSourceConfig:
			default:
				return errs.New(errs.KindValidationFailed, "backup terminal receipt source kind is invalid")
			}
		}
	case TaskBackupPrune:
		if len(record.Sources) != 0 || len(record.Points) == 0 ||
			len(record.Points) > backupruntime.MaximumBackupPruneDispatchPoints {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt points are invalid")
		}
		seen := make(map[string]struct{}, len(record.Points))
		for _, point := range record.Points {
			if backupruntime.ValidateBackupRecoveryPointSnapshot(point.Point) != nil ||
				point.Point.EnvironmentID != record.Task.Owner.EnvironmentID ||
				!backupruntime.ValidBackupRuntimeInstant(point.CreatedAt) ||
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
		domainDigest, err := backupTerminalDomainDigest(record.Points)
		if err != nil || domainDigest != record.DomainDigest {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt point digest is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task type is invalid")
	}
	receiptDigest, err := backupTerminalReceiptDigest(record)
	if err != nil {
		return err
	}
	if receiptDigest != record.ReceiptDigest {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt digest is invalid")
	}
	return nil
}

func validateBackupTerminalTaskEvidence(evidence BackupTerminalTaskEvidence) error {
	if recordcodec.ValidateID(ids.KindTask, evidence.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, evidence.OperationID) != nil ||
		(evidence.RetryOf != "" && recordcodec.ValidateID(ids.KindTask, evidence.RetryOf) != nil) ||
		validateTaskOwner(evidence.Owner) != nil || evidence.Owner.EnvironmentID == "" ||
		!validTaskActor(evidence.Actor) || !taskjournal.ValidExecutor(evidence.Executor) ||
		evidence.Executor != taskjournal.TaskExecutorAgent || evidence.Target != evidence.Owner.EnvironmentID ||
		recordcodec.ValidateID(ids.KindPlan, evidence.PlanID) != nil || !recordcodec.ValidSHA256(evidence.PlanHash) ||
		!isTerminalTaskStatus(evidence.Status) || !recordcodec.ValidSHA256(evidence.ResultDigest) ||
		!recordcodec.ValidSHA256(evidence.TaskDigest) || recordcodec.ValidateTimestamp("receipt created_at", evidence.CreatedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt updated_at", evidence.UpdatedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt finished_at", evidence.FinishedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt retain_until", evidence.RetainUntil) != nil ||
		!evidence.UpdatedAt.Equal(evidence.FinishedAt) ||
		!evidence.RetainUntil.Equal(evidence.FinishedAt.Add(TaskRetention)) {
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
	if evidence.TaskType == taskjournal.TaskBackupPrune && evidence.Actor != TaskActorSystem {
		return errs.New(errs.KindValidationFailed, "backup prune terminal receipt actor is invalid")
	}
	return nil
}
