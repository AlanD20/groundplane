package backupruntime

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PreparePostgresRestoreCheckpoint advances the native, immutable Restore
// projection. ApplyStart is an irreversible spent-attempt receipt: a later
// uncertainty may be held for recovery, never treated as authority to apply
// again. The common resume fold independently checks the complete wire order.
func PreparePostgresRestoreCheckpoint(current BackupRestoreRecord,
	request *agentpb.BackupCheckpointRequest, at time.Time,
) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceAttach ||
		request == nil || request.TaskId != current.TaskID ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore checkpoint identity is invalid")
	}
	if _, err := executionplan.ValidateBackupCheckpointRequest(request, request.CheckpointSequence); err != nil {
		return BackupRestoreRecord{}, err
	}
	next := CloneBackupRestoreRecord(current)
	next.UpdatedAt = at
	progress := next.PostgresProgress
	if progress == nil {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore progress is missing")
	}
	switch event := request.Checkpoint.(type) {
	case *agentpb.BackupCheckpointRequest_RestoreArtifactValidated:
		value := event.RestoreArtifactValidated
		if value == nil || current.State != BackupRestoreDownloading && current.State != BackupRestoreQueued ||
			value.PointId != current.Point.ID || !backupObjectMatchesWire(current.Point.Object, value.Object) ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, value.Evidence) ||
			value.GetPostgres() == nil || value.GetPostgres().PgDumpMajor != 16 ||
			value.GetPostgres().AdapterContractVersion != postgres16protocol.AdapterContractVersion {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore artifact differs from selected Point")
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
	case *agentpb.BackupCheckpointRequest_PostgresServiceProgress:
		value := event.PostgresServiceProgress
		if value == nil || progress.ContainerID == "" {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore Service progress lacks observed database")
		}
		if !progress.ApplyStarted {
			if err := advancePostgresRestoreStop(&next, value); err != nil {
				return BackupRestoreRecord{}, err
			}
		} else {
			if err := advancePostgresRestoreRecovery(&next, value); err != nil {
				return BackupRestoreRecord{}, err
			}
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
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore checkpoint is unsupported")
	}
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

func advancePostgresRestoreStop(next *BackupRestoreRecord, value *agentpb.BackupPostgresServiceProgress) error {
	progress := next.PostgresProgress
	if next.State != BackupRestoreArtifactVerified && next.State != BackupRestoreConsumersStopped ||
		progress.StopCursor >= next.ServiceCount ||
		value.ServiceCursor != progress.StopCursor ||
		value.ServiceId != next.CurrentTarget.Postgres.Consumers[progress.StopCursor].ServiceID {
		return invalidBackupRuntimeRecord("PostgreSQL Restore stop cursor changed")
	}
	consumer := next.CurrentTarget.Postgres.Consumers[progress.StopCursor]
	switch value.Phase {
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT:
		if consumer.PriorIntent != BackupServiceIntentRunning ||
			progress.StopPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT &&
				progress.StopServiceID == value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore stop intent is repeated")
		}
		progress.ServiceMutationStarted = true
		next.MutationStarted = true
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED:
		if consumer.PriorIntent != BackupServiceIntentRunning ||
			progress.StopPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT ||
			progress.StopServiceID != value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore stop receipt lacks intent")
		}
		progress.StopCursor++
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING:
		if consumer.PriorIntent == BackupServiceIntentRunning ||
			progress.StopPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT &&
				progress.StopServiceID == value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore non-running consumer changed")
		}
		progress.StopCursor++
	default:
		return invalidBackupRuntimeRecord("PostgreSQL Restore recovery phase preceded apply")
	}
	progress.StopPhase, progress.StopServiceID = value.Phase, value.ServiceId
	next.State = BackupRestoreConsumersStopped
	return nil
}

func advancePostgresRestoreRecovery(next *BackupRestoreRecord, value *agentpb.BackupPostgresServiceProgress) error {
	progress := next.PostgresProgress
	if next.State != BackupRestoreRestoring || progress.RestoreVerificationSHA256 == "" ||
		progress.RecoveryCursor >= next.ServiceCount {
		return invalidBackupRuntimeRecord("PostgreSQL Restore recovery precedes verification")
	}
	index := next.ServiceCount - progress.RecoveryCursor - 1
	consumer := next.CurrentTarget.Postgres.Consumers[index]
	if value.ServiceCursor != index || value.ServiceId != consumer.ServiceID {
		return invalidBackupRuntimeRecord("PostgreSQL Restore reverse Service cursor changed")
	}
	switch value.Phase {
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT:
		if consumer.PriorIntent != BackupServiceIntentRunning || progress.RecoveryServiceID == value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore restart intent changed")
		}
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED:
		if progress.RecoveryPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT ||
			progress.RecoveryServiceID != value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore restart lacks intent")
		}
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT:
		if progress.RecoveryPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED ||
			progress.RecoveryServiceID != value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore health wait lacks restart")
		}
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY:
		if progress.RecoveryPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT ||
			progress.RecoveryServiceID != value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore health proof lacks wait")
		}
		progress.RecoveryCursor++
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING:
		if consumer.PriorIntent == BackupServiceIntentRunning || progress.RecoveryServiceID == value.ServiceId {
			return invalidBackupRuntimeRecord("PostgreSQL Restore non-running consumer was restarted")
		}
		progress.RecoveryCursor++
	default:
		return invalidBackupRuntimeRecord("PostgreSQL Restore Service phase is invalid")
	}
	progress.RecoveryPhase, progress.RecoveryServiceID = value.Phase, value.ServiceId
	if progress.RecoveryCursor == next.ServiceCount {
		next.State = BackupRestoreConsumersRestored
	}
	return nil
}
