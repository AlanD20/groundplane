package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

type BackupOrphanRecord struct {
	Point          BackupRecoveryPointSnapshot         `json:"point"`
	TaskID         string                              `json:"task_id"`
	Reconciliation BackupOrphanReconciliationAuthority `json:"reconciliation"`
	State          BackupOrphanState                   `json:"state"`
	CreatedAt      time.Time                           `json:"created_at"`
	UpdatedAt      time.Time                           `json:"updated_at"`
}

type BackupOrphanReconciliationAuthority struct {
	OperationID    string `json:"operation_id"`
	PolicyRevision int64  `json:"policy_revision"`
	RetentionKeep  int64  `json:"retention_keep"`
}

type BackupRetentionSweepRecord struct {
	SourceID               string               `json:"source_id"`
	TriggerRecoveryPointID string               `json:"trigger_recovery_point_id"`
	Keep                   int64                `json:"keep"`
	Revision               int64                `json:"revision"`
	SelectionRevision      int64                `json:"selection_revision,omitempty"`
	Cursor                 string               `json:"cursor,omitempty"`
	RetainedCount          int64                `json:"retained_count"`
	PruneOperationID       string               `json:"prune_operation_id,omitempty"`
	State                  BackupRetentionState `json:"state"`
	CreatedAt              time.Time            `json:"created_at"`
	UpdatedAt              time.Time            `json:"updated_at"`
}

type BackupRecoveryPointPruneRecord struct {
	Point         BackupRecoveryPointSnapshot `json:"point"`
	PointRevision int64                       `json:"point_revision"`
	OperationID   string                      `json:"operation_id"`
	State         BackupPruneState            `json:"state"`
	TaskID        string                      `json:"task_id,omitempty"`
	CreatedAt     time.Time                   `json:"created_at"`
	UpdatedAt     time.Time                   `json:"updated_at"`
}

type BackupRecoveryPointPruneDispatchRecord struct {
	TaskID           string    `json:"task_id"`
	OperationID      string    `json:"operation_id"`
	EnvironmentID    string    `json:"environment_id"`
	RecoveryPointIDs []string  `json:"recovery_point_ids"`
	CreatedAt        time.Time `json:"created_at"`
}

func validateBackupOrphanRecord(record BackupOrphanRecord) error {
	if err := validateBackupRecoveryPointSnapshot(record.Point); err != nil {
		return err
	}
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.Reconciliation.OperationID) != nil ||
		record.Reconciliation.PolicyRevision <= 0 ||
		record.Reconciliation.RetentionKeep <= 0 ||
		record.Reconciliation.RetentionKeep > backuppolicy.MaximumBackupPolicyKeep ||
		(record.State != BackupOrphanInspect && record.State != BackupOrphanDelete) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) ||
		record.CreatedAt.Before(record.Point.CreatedAt) {
		return invalidBackupRuntimeRecord("backup orphan is invalid")
	}
	return nil
}

func validateBackupRetentionSweepRecord(record BackupRetentionSweepRecord) error {
	if recordcodec.ValidateID(ids.KindBackupSource, record.SourceID) != nil ||
		recordcodec.ValidateID(
			ids.KindRecoveryPoint,
			record.TriggerRecoveryPointID,
		) != nil || record.Keep <= 0 || record.Keep > backuppolicy.MaximumBackupPolicyKeep ||
		record.Revision <= 0 || !validBackupRetentionState(record.State) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup retention sweep is invalid")
	}
	if record.Cursor != "" && recordcodec.ValidateID(ids.KindRecoveryPoint, record.Cursor) != nil {
		return invalidBackupRuntimeRecord("backup retention cursor is invalid")
	}
	if record.RetainedCount < 0 || record.RetainedCount > record.Keep {
		return invalidBackupRuntimeRecord("backup retention retained count exceeds keep")
	}
	if record.State == BackupRetentionPending {
		if record.SelectionRevision != 0 || record.Cursor != "" || record.RetainedCount != 0 ||
			record.PruneOperationID != "" {
			return invalidBackupRuntimeRecord("pending backup retention sweep contains progress")
		}
	} else if record.SelectionRevision <= 0 ||
		recordcodec.ValidateID(ids.KindOperation, record.PruneOperationID) != nil {
		return invalidBackupRuntimeRecord(
			"active backup retention sweep requires a prune operation",
		)
	}
	return nil
}

func validateBackupRecoveryPointPruneRecord(record BackupRecoveryPointPruneRecord) error {
	if err := validateBackupRecoveryPointSnapshot(record.Point); err != nil {
		return err
	}
	if record.PointRevision <= 0 ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) ||
		record.CreatedAt.Before(record.Point.CreatedAt) {
		return invalidBackupRuntimeRecord("recovery point prune lifecycle is invalid")
	}
	switch record.State {
	case BackupPrunePending:
		if record.TaskID != "" {
			return invalidBackupRuntimeRecord("pending recovery point prune cannot carry a task id")
		}
	case BackupPruneAssigned, BackupPruneVerifiedAbsent:
		if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil {
			return invalidBackupRuntimeRecord("assigned recovery point prune requires a task id")
		}
	default:
		return invalidBackupRuntimeRecord("recovery point prune state is invalid")
	}
	return nil
}

func validateBackupRecoveryPointPruneDispatchRecord(
	record BackupRecoveryPointPruneDispatchRecord,
) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		!validBackupRuntimeInstant(record.CreatedAt) || len(record.RecoveryPointIDs) == 0 ||
		len(record.RecoveryPointIDs) > maximumBackupPruneDispatchPoints {
		return invalidBackupRuntimeRecord("recovery point prune dispatch is invalid")
	}
	seen := make(map[string]struct{}, len(record.RecoveryPointIDs))
	for _, recoveryPointID := range record.RecoveryPointIDs {
		if recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID) != nil {
			return invalidBackupRuntimeRecord("recovery point prune dispatch id is invalid")
		}
		if _, exists := seen[recoveryPointID]; exists {
			return invalidBackupRuntimeRecord("recovery point prune dispatch ids are not unique")
		}
		seen[recoveryPointID] = struct{}{}
	}
	return nil
}
