package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

func validateBackupWorkCheckpointPayload(request *agentpb.BackupWorkCheckpointRequest) error {
	valid := false
	switch checkpoint := request.Checkpoint.(type) {
	case *agentpb.BackupWorkCheckpointRequest_DatabaseConsumerRecovery:
		valid = checkpoint != nil && checkpoint.DatabaseConsumerRecovery != nil && backupCheckpointPoint(checkpoint.DatabaseConsumerRecovery.PointId)
	case *agentpb.BackupWorkCheckpointRequest_ArtifactPrepared:
		valid = checkpoint != nil && validBackupArtifactPrepared(checkpoint.ArtifactPrepared)
	case *agentpb.BackupWorkCheckpointRequest_UploadCompleted:
		valid = checkpoint != nil && validBackupUploadCompleted(checkpoint.UploadCompleted)
	case *agentpb.BackupWorkCheckpointRequest_UploadVerified:
		if checkpoint == nil {
			break
		}
		value := checkpoint.UploadVerified
		valid = value != nil && backupCheckpointPoint(value.PointId) &&
			validBackupCheckpointEvidence(value.Evidence) &&
			validBackupCheckpointObject(value.Object, value.PointId) &&
			validBackupCheckpointMetadata(value.MetadataCount, value.MetadataSha256)
	case *agentpb.BackupWorkCheckpointRequest_SourceCleanupCompleted:
		if checkpoint == nil {
			break
		}
		value := checkpoint.SourceCleanupCompleted
		valid = value != nil && backupCheckpointPoint(value.PointId) && validBackupCheckpointEvidence(value.Evidence)
	case *agentpb.BackupWorkCheckpointRequest_RestoreArtifactValidated:
		valid = checkpoint != nil && validBackupRestoreArtifact(checkpoint.RestoreArtifactValidated)
	case *agentpb.BackupWorkCheckpointRequest_PostgresContainerObserved:
		valid = checkpoint != nil && validBackupPostgresObservation(checkpoint.PostgresContainerObserved)
	case *agentpb.BackupWorkCheckpointRequest_PostgresDumpStart:
		valid = checkpoint != nil && validBackupPostgresDump(checkpoint.PostgresDumpStart)
	case *agentpb.BackupWorkCheckpointRequest_PostgresRestoreApplyStart:
		valid = checkpoint != nil && validBackupPostgresApply(checkpoint.PostgresRestoreApplyStart)
	case *agentpb.BackupWorkCheckpointRequest_PostgresRestoreVerified:
		if checkpoint == nil {
			break
		}
		value := checkpoint.PostgresRestoreVerified
		valid = value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointHexID(value.ContainerId) &&
			validBackupCheckpointEvidence(value.Evidence) && backupCheckpointDigest(value.VerificationSha256)
	case *agentpb.BackupWorkCheckpointRequest_PostgresServiceProgress:
		valid = checkpoint != nil && validBackupPostgresService(checkpoint.PostgresServiceProgress)
	case *agentpb.BackupWorkCheckpointRequest_MysqlContainerObserved:
		valid = checkpoint != nil && validBackupMySQLObservation(checkpoint.MysqlContainerObserved)
	case *agentpb.BackupWorkCheckpointRequest_MysqlDumpStart:
		valid = checkpoint != nil && validBackupMySQLDump(checkpoint.MysqlDumpStart)
	case *agentpb.BackupWorkCheckpointRequest_MysqlRestoreApplyStart:
		valid = checkpoint != nil && validBackupMySQLApply(checkpoint.MysqlRestoreApplyStart)
	case *agentpb.BackupWorkCheckpointRequest_MysqlRestoreVerified:
		if checkpoint == nil {
			break
		}
		value := checkpoint.MysqlRestoreVerified
		valid = value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointHexID(value.ContainerId) &&
			validBackupCheckpointEvidence(value.Evidence) && backupCheckpointDigest(value.VerificationSha256)
	case *agentpb.BackupWorkCheckpointRequest_MysqlServiceProgress:
		valid = checkpoint != nil && validBackupMySQLService(checkpoint.MysqlServiceProgress)
	case *agentpb.BackupWorkCheckpointRequest_Config:
		valid = checkpoint != nil && validBackupConfigCheckpoint(checkpoint.Config)
	case *agentpb.BackupWorkCheckpointRequest_Volume:
		valid = checkpoint != nil && validBackupVolumeCheckpoint(checkpoint.Volume)
	case *agentpb.BackupWorkCheckpointRequest_PruneObjectDeleted:
		if checkpoint == nil {
			break
		}
		value := checkpoint.PruneObjectDeleted
		valid = value != nil && value.Ordinal > 0 && backupCheckpointPoint(value.PointId) &&
			validBackupCheckpointObject(value.Object, value.PointId)
	}
	if !valid {
		return invalidBackupCheckpoint("typed payload is invalid")
	}
	return nil
}
