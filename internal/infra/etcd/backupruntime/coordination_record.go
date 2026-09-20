package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

type BackupScheduleCursorRecord struct {
	EnvironmentID   string    `json:"environment_id"`
	PolicyRevision  int64     `json:"policy_revision"`
	Frequency       string    `json:"frequency"`
	EnabledAt       time.Time `json:"enabled_at"`
	LastEvaluatedAt time.Time `json:"last_evaluated_at"`
	NextDueAt       time.Time `json:"next_due_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// EnvironmentMutationEpochRecord deliberately carries no counter. Rewriting
// this canonical value makes the etcd modification revision the mutation epoch.
type EnvironmentMutationEpochRecord struct {
	EnvironmentID string `json:"environment_id"`
}

type BackupDueOutcomeRecord struct {
	EnvironmentID  string           `json:"environment_id"`
	PolicyRevision int64            `json:"policy_revision"`
	ScheduledAt    time.Time        `json:"scheduled_at"`
	Outcome        BackupDueOutcome `json:"outcome"`
	TaskID         string           `json:"task_id,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	RetainUntil    time.Time        `json:"retain_until"`
}

type BackupOperationLockRecord struct {
	EnvironmentID string              `json:"environment_id"`
	OperationID   string              `json:"operation_id"`
	TaskID        string              `json:"task_id"`
	Kind          BackupOperationKind `json:"kind"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
}

type BackupSourceTargetExclusionRecord struct {
	EnvironmentID string                 `json:"environment_id"`
	OperationID   string                 `json:"operation_id"`
	TaskID        string                 `json:"task_id"`
	OperationKind BackupOperationKind    `json:"operation_kind"`
	TargetKind    BackupSourceTargetKind `json:"target_kind"`
	TargetID      string                 `json:"target_id"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

func validateBackupScheduleCursorRecord(record BackupScheduleCursorRecord) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if record.PolicyRevision <= 0 || backuppolicy.ValidateFrequency(record.Frequency) != nil ||
		!ValidBackupRuntimeInstant(
			record.EnabledAt,
		) || !ValidBackupRuntimeInstant(record.LastEvaluatedAt) ||
		!ValidBackupRuntimeInstant(
			record.NextDueAt,
		) || !ValidBackupRuntimeInstant(record.UpdatedAt) ||
		record.LastEvaluatedAt.Before(
			record.EnabledAt,
		) || !record.NextDueAt.After(record.LastEvaluatedAt) ||
		record.UpdatedAt.Before(record.EnabledAt) {
		return invalidBackupRuntimeRecord("backup schedule cursor is invalid")
	}
	return nil
}

func ValidateEnvironmentMutationEpochRecord(record EnvironmentMutationEpochRecord) error {
	if recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil {
		return invalidBackupRuntimeRecord("environment mutation epoch is invalid")
	}
	return nil
}

func validateBackupDueOutcomeRecord(record BackupDueOutcomeRecord) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if record.PolicyRevision <= 0 || !ValidBackupRuntimeInstant(record.ScheduledAt) ||
		!ValidBackupRuntimeInstant(
			record.CreatedAt,
		) || !ValidBackupRuntimeInstant(record.RetainUntil) ||
		record.ScheduledAt.After(record.CreatedAt) || !record.RetainUntil.After(record.CreatedAt) {
		return invalidBackupRuntimeRecord("backup due outcome lifecycle is invalid")
	}
	switch record.Outcome {
	case BackupDueDispatched:
		if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil {
			return invalidBackupRuntimeRecord("dispatched backup due outcome requires a task id")
		}
	case BackupDueSkippedOverlap:
		if record.TaskID != "" {
			return invalidBackupRuntimeRecord("skipped backup due outcome cannot carry a task id")
		}
	default:
		return invalidBackupRuntimeRecord("backup due outcome is invalid")
	}
	return nil
}

func validateBackupOperationLockRecord(record BackupOperationLockRecord) error {
	if recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(
			ids.KindTask,
			record.TaskID,
		) != nil || !validBackupOperationKind(record.Kind) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup operation lock is invalid")
	}
	return nil
}

func validateBackupSourceTargetExclusionRecord(record BackupSourceTargetExclusionRecord) error {
	if recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		(record.OperationKind != BackupOperationBackup && record.OperationKind != BackupOperationRestore) ||
		validateBackupSourceTargetIdentity(record.TargetKind, record.TargetID) != nil ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup source-target exclusion is invalid")
	}
	return nil
}
