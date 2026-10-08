package backupruntime

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareDatabaseRestoreCheckpoint advances the native, immutable Restore
// projection. ApplyStart is an irreversible spent-attempt receipt: a later
// uncertainty may be held for recovery, never treated as authority to apply
// again. The common resume fold independently checks the complete wire order.
func PrepareDatabaseRestoreCheckpoint(current BackupRestoreRecord,
	request *agentpb.BackupCheckpointRequest, at time.Time,
) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceAttach ||
		request == nil || request.TaskId != current.TaskID ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("database Restore checkpoint identity is invalid")
	}
	if _, err := executionplan.ValidateBackupCheckpointRequest(request, request.CheckpointSequence); err != nil {
		return BackupRestoreRecord{}, err
	}
	next := CloneBackupRestoreRecord(current)
	next.UpdatedAt = at
	progress := next.DatabaseProgress
	if progress == nil {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("database Restore progress is missing")
	}
	switch event := request.Checkpoint.(type) {
	case *agentpb.BackupCheckpointRequest_RestoreArtifactValidated:
		value := event.RestoreArtifactValidated
		if value == nil || current.State != BackupRestoreDownloading && current.State != BackupRestoreQueued ||
			value.PointId != current.Point.ID || !backupObjectMatchesWire(current.Point.Object, value.Object) ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, value.Evidence) ||
			!databaseRestoreArchiveMatches(current, value) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("database Restore artifact differs from selected Point")
		}
		artifact := current.Point.Evidence
		next.Artifact, next.State = &artifact, BackupRestoreArtifactVerified
	case *agentpb.BackupCheckpointRequest_PostgresContainerObserved:
		value := event.PostgresContainerObserved
		if value == nil || current.State != BackupRestoreArtifactVerified || progress.ContainerID != "" ||
			value.ServiceId != current.CurrentTarget.Postgres.Source.BackingServiceID {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore container observation is out of order")
		}
		progress.ContainerID = value.ContainerId
		progress.ObservationSHA256 = hex.EncodeToString(value.ObservationSha256)
		progress.ObservedRepositoryDigest = hex.EncodeToString(value.RepositoryDigest)
		progress.ObservedLabelsSHA256 = hex.EncodeToString(value.ObservedLabelsSha256)
	case *agentpb.BackupCheckpointRequest_MysqlContainerObserved:
		value := event.MysqlContainerObserved
		if value == nil || current.State != BackupRestoreArtifactVerified || progress.ContainerID != "" ||
			current.CurrentTarget.MySQL == nil ||
			value.ServiceId != current.CurrentTarget.MySQL.Source.BackingServiceID {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("MySQL Restore container observation is out of order")
		}
		progress.ContainerID = value.ContainerId
		progress.ObservationSHA256 = hex.EncodeToString(value.ObservationSha256)
		progress.ObservedRepositoryDigest = hex.EncodeToString(value.ImageReferenceSha256)
		progress.ObservedLabelsSHA256 = hex.EncodeToString(value.ObservedLabelsSha256)
	case *agentpb.BackupCheckpointRequest_PostgresServiceProgress:
		value := event.PostgresServiceProgress
		if value == nil || progress.ContainerID == "" {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore Service progress lacks observed database")
		}
		if !progress.ApplyStarted {
			if err := advanceDatabaseRestoreStop(&next, databaseServiceCheckpoint{
				cursor: value.ServiceCursor, serviceID: value.ServiceId, phase: value.Phase,
			}); err != nil {
				return BackupRestoreRecord{}, err
			}
		} else {
			if err := advanceDatabaseRestoreRecovery(&next, databaseServiceCheckpoint{
				cursor: value.ServiceCursor, serviceID: value.ServiceId, phase: value.Phase,
			}); err != nil {
				return BackupRestoreRecord{}, err
			}
		}
	case *agentpb.BackupCheckpointRequest_MysqlServiceProgress:
		value := event.MysqlServiceProgress
		if value == nil || progress.ContainerID == "" {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("MySQL Restore Service progress lacks observed database")
		}
		checkpoint := databaseServiceCheckpoint{cursor: value.ServiceCursor, serviceID: value.ServiceId, phase: value.Phase}
		if !progress.ApplyStarted {
			if err := advanceDatabaseRestoreStop(&next, checkpoint); err != nil {
				return BackupRestoreRecord{}, err
			}
		} else if err := advanceDatabaseRestoreRecovery(&next, checkpoint); err != nil {
			return BackupRestoreRecord{}, err
		}
	case *agentpb.BackupCheckpointRequest_PostgresRestoreApplyStart:
		value := event.PostgresRestoreApplyStart
		if value == nil || current.State != BackupRestoreConsumersStopped || progress.ApplyStarted ||
			progress.StopCursor != current.ServiceCount || value.PointId != current.Point.ID ||
			value.ContainerId != progress.ContainerID ||
			hex.EncodeToString(value.RepositoryDigest) != progress.ObservedRepositoryDigest ||
			hex.EncodeToString(value.ExpectedLabelsSha256) != progress.ObservedLabelsSHA256 ||
			value.SourceSizeBytes != current.Point.Evidence.SourceSizeBytes ||
			hex.EncodeToString(value.SourceSha256) != current.Point.Evidence.SourceSHA256 {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore apply start changed selected input")
		}
		progress.ApplyStarted = true
		progress.ApplyExecutionNonce = hex.EncodeToString(value.ExecutionNonce)
		progress.ApplyExecID = value.ExecId
		progress.ApplyRepositoryDigest = hex.EncodeToString(value.RepositoryDigest)
		progress.ApplyLabelsSHA256 = hex.EncodeToString(value.ExpectedLabelsSha256)
		next.MutationStarted, next.State = true, BackupRestoreRestoring
	case *agentpb.BackupCheckpointRequest_MysqlRestoreApplyStart:
		value := event.MysqlRestoreApplyStart
		if value == nil || current.State != BackupRestoreConsumersStopped || progress.ApplyStarted ||
			progress.StopCursor != current.ServiceCount || value.PointId != current.Point.ID ||
			value.ContainerId != progress.ContainerID ||
			hex.EncodeToString(value.ImageReferenceSha256) != progress.ObservedRepositoryDigest ||
			hex.EncodeToString(value.ExpectedLabelsSha256) != progress.ObservedLabelsSHA256 ||
			value.SourceSizeBytes != current.Point.Evidence.SourceSizeBytes ||
			hex.EncodeToString(value.SourceSha256) != current.Point.Evidence.SourceSHA256 {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("MySQL Restore apply start changed selected input")
		}
		progress.ApplyStarted = true
		progress.ApplyExecutionNonce = hex.EncodeToString(value.ExecutionNonce)
		progress.ApplyExecID = value.ExecId
		progress.ApplyRepositoryDigest = hex.EncodeToString(value.ImageReferenceSha256)
		progress.ApplyLabelsSHA256 = hex.EncodeToString(value.ExpectedLabelsSha256)
		next.MutationStarted, next.State = true, BackupRestoreRestoring
	case *agentpb.BackupCheckpointRequest_PostgresRestoreVerified:
		value := event.PostgresRestoreVerified
		if value == nil || current.State != BackupRestoreRestoring || !progress.ApplyStarted ||
			progress.RestoreVerificationSHA256 != "" || value.PointId != current.Point.ID ||
			value.ContainerId != progress.ContainerID ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, value.Evidence) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore verification does not prove the spent apply")
		}
		progress.RestoreVerificationSHA256 = hex.EncodeToString(value.VerificationSha256)
		if current.ServiceCount == 0 {
			next.State = BackupRestoreConsumersRestored
		}
	case *agentpb.BackupCheckpointRequest_MysqlRestoreVerified:
		value := event.MysqlRestoreVerified
		if value == nil || current.State != BackupRestoreRestoring || !progress.ApplyStarted ||
			progress.RestoreVerificationSHA256 != "" || value.PointId != current.Point.ID ||
			value.ContainerId != progress.ContainerID ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, value.Evidence) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("MySQL Restore verification does not prove the spent apply")
		}
		progress.RestoreVerificationSHA256 = hex.EncodeToString(value.VerificationSha256)
		if current.ServiceCount == 0 {
			next.State = BackupRestoreConsumersRestored
		}
	case *agentpb.BackupCheckpointRequest_SourceCleanupCompleted:
		value := event.SourceCleanupCompleted
		if value == nil || current.State != BackupRestoreConsumersRestored ||
			progress.SourceCleanupCompleted || value.PointId != current.Point.ID ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, value.Evidence) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore source cleanup is out of order")
		}
		progress.SourceCleanupCompleted = true
		next.State, next.Verification = BackupRestoreVerified, BackupVerificationPassed
	default:
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("database Restore checkpoint is unsupported")
	}
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

type databaseServiceCheckpoint struct {
	cursor    uint32
	serviceID string
	phase     agentpb.BackupServicePhase
}

func databaseRestoreArchiveMatches(current BackupRestoreRecord, value *agentpb.BackupRestoreArtifactValidated) bool {
	switch current.Point.SourceFormat {
	case BackupRuntimeFormatPostgres:
		archive, err := PostgresArchiveEvidenceFromWire(value.GetPostgres())
		return err == nil && value.GetMysql() == nil && archive == current.Point.PostgresArchive
	case BackupRuntimeFormatMySQL:
		archive, err := MySQLArchiveEvidenceFromWire(value.GetMysql())
		return err == nil && value.GetPostgres() == nil && archive == current.Point.MySQLArchive
	default:
		return false
	}
}

func databaseRestoreConsumers(record *BackupRestoreRecord) []BackupRestoreDatabaseServiceSnapshot {
	if record.CurrentTarget.Postgres != nil {
		return record.CurrentTarget.Postgres.Consumers
	}
	if record.CurrentTarget.MySQL != nil {
		return record.CurrentTarget.MySQL.Consumers
	}
	return nil
}

func advanceDatabaseRestoreStop(next *BackupRestoreRecord, value databaseServiceCheckpoint) error {
	progress := next.DatabaseProgress
	consumers := databaseRestoreConsumers(next)
	if next.State != BackupRestoreArtifactVerified && next.State != BackupRestoreConsumersStopped ||
		progress.StopCursor >= next.ServiceCount ||
		value.cursor != progress.StopCursor || int(progress.StopCursor) >= len(consumers) ||
		value.serviceID != consumers[progress.StopCursor].ServiceID {
		return invalidBackupRuntimeRecord("PostgreSQL Restore stop cursor changed")
	}
	consumer := consumers[progress.StopCursor]
	switch value.phase {
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT:
		if consumer.PriorIntent != BackupServiceIntentRunning ||
			progress.StopPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT &&
				progress.StopServiceID == value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore stop intent is repeated")
		}
		progress.ServiceMutationStarted = true
		next.MutationStarted = true
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED:
		if consumer.PriorIntent != BackupServiceIntentRunning ||
			progress.StopPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT ||
			progress.StopServiceID != value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore stop receipt lacks intent")
		}
		progress.StopCursor++
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING:
		if consumer.PriorIntent == BackupServiceIntentRunning ||
			progress.StopPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT &&
				progress.StopServiceID == value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore non-running consumer changed")
		}
		progress.StopCursor++
	default:
		return invalidBackupRuntimeRecord("PostgreSQL Restore recovery phase preceded apply")
	}
	progress.StopPhase, progress.StopServiceID = value.phase, value.serviceID
	next.State = BackupRestoreConsumersStopped
	return nil
}

func advanceDatabaseRestoreRecovery(next *BackupRestoreRecord, value databaseServiceCheckpoint) error {
	progress := next.DatabaseProgress
	consumers := databaseRestoreConsumers(next)
	if next.State != BackupRestoreRestoring || progress.RestoreVerificationSHA256 == "" ||
		progress.RecoveryCursor >= next.ServiceCount {
		return invalidBackupRuntimeRecord("PostgreSQL Restore recovery precedes verification")
	}
	index := next.ServiceCount - progress.RecoveryCursor - 1
	if int(index) >= len(consumers) {
		return invalidBackupRuntimeRecord("database Restore consumer set changed")
	}
	consumer := consumers[index]
	if value.cursor != index || value.serviceID != consumer.ServiceID {
		return invalidBackupRuntimeRecord("PostgreSQL Restore reverse Service cursor changed")
	}
	switch value.phase {
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT:
		if consumer.PriorIntent != BackupServiceIntentRunning || progress.RecoveryServiceID == value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore restart intent changed")
		}
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED:
		if progress.RecoveryPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT ||
			progress.RecoveryServiceID != value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore restart lacks intent")
		}
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT:
		if progress.RecoveryPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED ||
			progress.RecoveryServiceID != value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore health wait lacks restart")
		}
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY:
		if progress.RecoveryPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT ||
			progress.RecoveryServiceID != value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore health proof lacks wait")
		}
		progress.RecoveryCursor++
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING:
		if consumer.PriorIntent == BackupServiceIntentRunning || progress.RecoveryServiceID == value.serviceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore non-running consumer was restarted")
		}
		progress.RecoveryCursor++
	default:
		return invalidBackupRuntimeRecord("PostgreSQL Restore Service phase is invalid")
	}
	progress.RecoveryPhase, progress.RecoveryServiceID = value.phase, value.serviceID
	if progress.RecoveryCursor == next.ServiceCount {
		next.State = BackupRestoreConsumersRestored
	}
	return nil
}
