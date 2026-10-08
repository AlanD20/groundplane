package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

type BackupOrphanRecord struct {
	Target                      BackupRecoveryPointTargetSnapshot   `json:"target"`
	Evidence                    BackupArtifactEvidence              `json:"evidence"`
	ConfigArchive               BackupConfigArchiveEvidence         `json:"config_archive"`
	VolumeArchive               BackupVolumeArchiveEvidence         `json:"volume_archive"`
	PostgresArchive             BackupPostgresArchiveEvidence       `json:"postgres_archive"`
	MySQLArchive                BackupMySQLArchiveEvidence          `json:"mysql_archive"`
	Upload                      BackupUploadOutcome                 `json:"upload"`
	Object                      BackupObjectIdentity                `json:"object"`
	Postgres                    BackupPostgresPointIdentity         `json:"postgres"`
	MySQL                       BackupMySQLPointIdentity            `json:"mysql"`
	CleanupProof                BackupOrphanCleanupProof            `json:"cleanup_proof"`
	UnknownResolvedByReconciler bool                                `json:"unknown_resolved_by_reconciler"`
	Phase                       BackupSourceAttemptPhase            `json:"phase"`
	TaskID                      string                              `json:"task_id"`
	Reconciliation              BackupOrphanReconciliationAuthority `json:"reconciliation"`
	State                       BackupOrphanState                   `json:"state"`
	CreatedAt                   time.Time                           `json:"created_at"`
	UpdatedAt                   time.Time                           `json:"updated_at"`
}

type BackupOrphanReconciliationAuthority struct {
	OperationID    string `json:"operation_id"`
	PolicyRevision int64  `json:"policy_revision"`
	PolicySHA256   string `json:"policy_sha256"`
	RetentionKeep  int64  `json:"retention_keep"`
}

// BackupOrphanCleanupProof is copied from one acknowledged Agent startup
// inventory while its original terminal Task and staging index still exist.
// It is independent of Task retention after the guarded native write.
type BackupOrphanCleanupProof struct {
	TaskID            string `json:"task_id"`
	TaskRevision      int64  `json:"task_revision"`
	StepID            string `json:"step_id"`
	RecoveryKeySHA256 string `json:"recovery_key_sha256"`
	AgentID           string `json:"agent_id"`
	AgentGeneration   uint64 `json:"agent_generation"`
	DeliveryRevision  int64  `json:"delivery_revision"`
	DeliverySHA256    string `json:"delivery_sha256"`
	Disposition       string `json:"disposition"`
}

type BackupRetentionSweepRecord struct {
	SourceID               string               `json:"source_id"`
	TriggerRecoveryPointID string               `json:"trigger_recovery_point_id"`
	Keep                   int64                `json:"keep"`
	Revision               int64                `json:"revision"`
	PolicySHA256           string               `json:"policy_sha256"`
	SelectionRevision      int64                `json:"selection_revision,omitempty"`
	Cursor                 string               `json:"cursor,omitempty"`
	RetainedCount          int64                `json:"retained_count"`
	PruneOperationID       string               `json:"prune_operation_id,omitempty"`
	State                  BackupRetentionState `json:"state"`
	CreatedAt              time.Time            `json:"created_at"`
	UpdatedAt              time.Time            `json:"updated_at"`
}

type BackupRecoveryPointPruneRecord struct {
	Point            BackupRecoveryPointSnapshot `json:"point"`
	PointRevision    int64                       `json:"point_revision"`
	PolicyRevision   int64                       `json:"policy_revision"`
	PolicySHA256     string                      `json:"policy_sha256"`
	OperationID      string                      `json:"operation_id"`
	DispatchAttempts uint8                       `json:"dispatch_attempts,omitempty"`
	State            BackupPruneState            `json:"state"`
	TaskID           string                      `json:"task_id,omitempty"`
	CreatedAt        time.Time                   `json:"created_at"`
	UpdatedAt        time.Time                   `json:"updated_at"`
}

type BackupRecoveryPointPruneDispatchRecord struct {
	TaskID           string    `json:"task_id"`
	OperationID      string    `json:"operation_id"`
	EnvironmentID    string    `json:"environment_id"`
	RecoveryPointIDs []string  `json:"recovery_point_ids"`
	CreatedAt        time.Time `json:"created_at"`
}

func validateBackupOrphanRecord(record BackupOrphanRecord) error {
	if err := ValidateBackupRecoveryPointTargetSnapshot(record.Target); err != nil {
		return err
	}
	if err := validateSelectedConfigArchive(record.Target.SourceKind, record.ConfigArchive, record.Evidence); err != nil {
		return err
	}
	if err := validateSelectedVolumeArchive(record.Target.SourceKind, record.VolumeArchive, record.Evidence); err != nil {
		return err
	}
	if err := validateSelectedMySQLArchive(record.Target.SourceKind, record.Target.SourceFormat, record.MySQLArchive); err != nil {
		return err
	}
	if err := validateSelectedPostgresArchive(record.Target.SourceKind, record.Target.SourceFormat, record.PostgresArchive); err != nil {
		return err
	}
	if !validBackupArtifactForTarget(record.Evidence, record.Target, record.PostgresArchive, record.MySQLArchive) ||
		record.Upload.Target != record.Target.ObjectTarget() ||
		!validBackupSourceArtifactState(
			BackupSourceAttemptOrphaned,
			record.Phase,
			record.Evidence,
			record.Upload,
			record.Object,
		) {
		return invalidBackupRuntimeRecord("backup orphan upload evidence or identity is invalid")
	}
	if record.Target.SourceKind == BackupRuntimeSourceAttach &&
		record.Target.SourceFormat == BackupRuntimeFormatPostgres {
		if !postgresidentity.ValidGenerated(record.Postgres.Database) ||
			!postgresidentity.ValidGenerated(record.Postgres.Role) ||
			recordcodec.ValidateID(ids.KindEnvironment, record.Postgres.BackingEnvironmentID) != nil ||
			recordcodec.ValidateID(ids.KindService, record.Postgres.BackingServiceID) != nil ||
			recordcodec.ValidateID(ids.KindService, record.Postgres.ConsumerServiceID) != nil {
			return invalidBackupRuntimeRecord("postgres backup orphan target identity is incomplete")
		}
	} else if record.Postgres != (BackupPostgresPointIdentity{}) {
		return invalidBackupRuntimeRecord("non-postgres backup orphan carries database identity")
	}
	if record.Target.SourceKind == BackupRuntimeSourceAttach && record.Target.SourceFormat == BackupRuntimeFormatMySQL {
		if !mysql84protocol.ValidGeneratedIdentity(record.MySQL.Database) ||
			!mysql84protocol.ValidGeneratedIdentity(record.MySQL.Role) ||
			recordcodec.ValidateID(ids.KindEnvironment, record.MySQL.BackingEnvironmentID) != nil ||
			recordcodec.ValidateID(ids.KindService, record.MySQL.BackingServiceID) != nil ||
			recordcodec.ValidateID(ids.KindService, record.MySQL.ConsumerServiceID) != nil {
			return invalidBackupRuntimeRecord("MySQL backup orphan target identity is incomplete")
		}
	} else if record.MySQL != (BackupMySQLPointIdentity{}) {
		return invalidBackupRuntimeRecord("non-MySQL backup orphan carries database identity")
	}
	if proof := record.CleanupProof; proof != (BackupOrphanCleanupProof{}) {
		if proof.TaskID != record.TaskID || proof.TaskRevision <= 0 ||
			recordcodec.ValidateID(ids.KindStep, proof.StepID) != nil ||
			!recordcodec.ValidSHA256(proof.RecoveryKeySHA256) ||
			recordcodec.ValidateID(ids.KindAgent, proof.AgentID) != nil || proof.AgentGeneration == 0 ||
			proof.DeliveryRevision <= proof.TaskRevision ||
			!recordcodec.ValidSHA256(proof.DeliverySHA256) ||
			(proof.Disposition != "discarded" && proof.Disposition != "absent") {
			return invalidBackupRuntimeRecord("backup orphan cleanup proof is incomplete")
		}
	}
	if record.UnknownResolvedByReconciler &&
		(record.CleanupProof == (BackupOrphanCleanupProof{}) || record.Upload.Kind != BackupUploadUnknown ||
			record.Object == (BackupObjectIdentity{}) || record.Phase != BackupSourcePhasePointCommit) {
		return invalidBackupRuntimeRecord("backup orphan unknown-upload resolution is invalid")
	}
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.Reconciliation.OperationID) != nil ||
		record.Reconciliation.PolicyRevision <= 0 || !recordcodec.ValidSHA256(record.Reconciliation.PolicySHA256) ||
		record.Reconciliation.RetentionKeep <= 0 ||
		record.Reconciliation.RetentionKeep > backuppolicy.MaximumBackupPolicyKeep ||
		(record.State != BackupOrphanInspect && record.State != BackupOrphanDelete) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) ||
		record.CreatedAt.Before(record.Target.CreatedAt) {
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
		record.Revision <= 0 || !recordcodec.ValidSHA256(record.PolicySHA256) || !validBackupRetentionState(record.State) ||
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
	if err := ValidateBackupRecoveryPointSnapshot(record.Point); err != nil {
		return err
	}
	if record.PointRevision <= 0 || record.PolicyRevision <= 0 || !recordcodec.ValidSHA256(record.PolicySHA256) ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) ||
		record.CreatedAt.Before(record.Point.CreatedAt) ||
		record.DispatchAttempts > MaximumBackupPruneDispatchAttempts {
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

func ValidateBackupRecoveryPointPruneDispatchRecord(
	record BackupRecoveryPointPruneDispatchRecord,
) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		!ValidBackupRuntimeInstant(record.CreatedAt) || len(record.RecoveryPointIDs) == 0 ||
		len(record.RecoveryPointIDs) > MaximumBackupPruneDispatchPoints {
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
