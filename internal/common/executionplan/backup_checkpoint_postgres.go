package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func backupCheckpointHexID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func backupCheckpointServiceID(value string) bool {
	return ids.Validate(ids.KindService, value) == nil || ids.Validate(ids.KindBackingService, value) == nil
}

func validBackupPostgresObservation(value *agentpb.BackupPostgresContainerObserved) bool {
	return value != nil && backupCheckpointServiceID(value.ServiceId) && backupCheckpointHexID(value.ContainerId) &&
		backupCheckpointDigest(value.RepositoryDigest) && value.ObservedLabelCount > 0 &&
		backupCheckpointDigest(value.ObservedLabelsSha256) && backupCheckpointDigest(value.ObservationSha256) &&
		backupCheckpointDigest(value.DatabaseImageIdSha256)
}

func validBackupPostgresDump(value *agentpb.BackupPostgresDumpStart) bool {
	return value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointDigest(value.ExecutionNonce) &&
		backupCheckpointHexID(value.ContainerId) && backupCheckpointHexID(value.ExecId) &&
		backupCheckpointDigest(value.RepositoryDigest) && backupCheckpointDigest(value.ExpectedLabelsSha256) &&
		value.AdapterContractVersion == postgres16protocol.AdapterContractVersion &&
		postgresidentity.ValidGenerated(value.DatabaseName) && postgresidentity.ValidGenerated(value.RoleName) &&
		(value.MaxPlaintextBytes == backupformat.MaxStoredBytes || value.MaxPlaintextBytes == backupformat.MaxAgeSourceBytes)
}

func validBackupPostgresApply(value *agentpb.BackupPostgresRestoreApplyStartCheckpoint) bool {
	return value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointDigest(value.ExecutionNonce) &&
		backupCheckpointHexID(value.ContainerId) && backupCheckpointHexID(value.ExecId) &&
		backupCheckpointDigest(value.RepositoryDigest) && backupCheckpointDigest(value.ExpectedLabelsSha256) &&
		value.SourceSizeBytes > 0 && value.SourceSizeBytes <= backupformat.MaxStoredBytes && backupCheckpointDigest(value.SourceSha256)
}

func backupCheckpointServicePhase(value agentpb.BackupServicePhase) bool {
	switch value {
	case agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING,
		agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT,
		agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED,
		agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT,
		agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED,
		agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT,
		agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY:
		return true
	default:
		return false
	}
}

func validBackupPostgresService(value *agentpb.BackupPostgresServiceProgress) bool {
	return value != nil && backupCheckpointServiceID(value.ServiceId) &&
		backupCheckpointServicePhase(value.Phase) && backupCheckpointDigest(value.ObservationSha256)
}
