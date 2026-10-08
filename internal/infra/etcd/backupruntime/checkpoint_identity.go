package backupruntime

import "github.com/AlanD20/groundplane/proto/agentpb"

// BackupCheckpointTag is the persisted closed schema-one checkpoint family.
// Callers validate the message before using this projection for acceptance.
func BackupCheckpointTag(request *agentpb.BackupCheckpointRequest) uint32 {
	if request == nil {
		return 0
	}
	switch request.Checkpoint.(type) {
	case *agentpb.BackupCheckpointRequest_ArtifactPrepared:
		return 20
	case *agentpb.BackupCheckpointRequest_UploadVerified:
		return 21
	case *agentpb.BackupCheckpointRequest_SourceCleanupCompleted:
		return 22
	case *agentpb.BackupCheckpointRequest_PostgresContainerObserved:
		return 23
	case *agentpb.BackupCheckpointRequest_PostgresDumpStart:
		return 24
	case *agentpb.BackupCheckpointRequest_PostgresRestoreApplyStart:
		return 25
	case *agentpb.BackupCheckpointRequest_RestoreArtifactValidated:
		return 26
	case *agentpb.BackupCheckpointRequest_Config:
		return 27
	case *agentpb.BackupCheckpointRequest_Volume:
		return 28
	case *agentpb.BackupCheckpointRequest_PruneObjectDeleted:
		return 29
	case *agentpb.BackupCheckpointRequest_PostgresRestoreVerified:
		return 30
	case *agentpb.BackupCheckpointRequest_PostgresServiceProgress:
		return 31
	case *agentpb.BackupCheckpointRequest_UploadCompleted:
		return 32
	case *agentpb.BackupCheckpointRequest_MysqlContainerObserved:
		return 33
	case *agentpb.BackupCheckpointRequest_MysqlDumpStart:
		return 34
	case *agentpb.BackupCheckpointRequest_MysqlRestoreApplyStart:
		return 35
	case *agentpb.BackupCheckpointRequest_MysqlRestoreVerified:
		return 36
	case *agentpb.BackupCheckpointRequest_MysqlServiceProgress:
		return 37
	default:
		return 0
	}
}

// BackupCheckpointPointID is a projection only. Config and Service progress
// identify their source through the sealed step, not through an invented point.
func BackupCheckpointPointID(request *agentpb.BackupCheckpointRequest) string {
	if request == nil {
		return ""
	}
	switch checkpoint := request.Checkpoint.(type) {
	case *agentpb.BackupCheckpointRequest_ArtifactPrepared:
		return checkpoint.ArtifactPrepared.GetPointId()
	case *agentpb.BackupCheckpointRequest_UploadCompleted:
		return checkpoint.UploadCompleted.GetPointId()
	case *agentpb.BackupCheckpointRequest_UploadVerified:
		return checkpoint.UploadVerified.GetPointId()
	case *agentpb.BackupCheckpointRequest_SourceCleanupCompleted:
		return checkpoint.SourceCleanupCompleted.GetPointId()
	case *agentpb.BackupCheckpointRequest_PostgresDumpStart:
		return checkpoint.PostgresDumpStart.GetPointId()
	case *agentpb.BackupCheckpointRequest_PostgresRestoreApplyStart:
		return checkpoint.PostgresRestoreApplyStart.GetPointId()
	case *agentpb.BackupCheckpointRequest_RestoreArtifactValidated:
		return checkpoint.RestoreArtifactValidated.GetPointId()
	case *agentpb.BackupCheckpointRequest_PruneObjectDeleted:
		return checkpoint.PruneObjectDeleted.GetPointId()
	case *agentpb.BackupCheckpointRequest_PostgresRestoreVerified:
		return checkpoint.PostgresRestoreVerified.GetPointId()
	case *agentpb.BackupCheckpointRequest_MysqlDumpStart:
		return checkpoint.MysqlDumpStart.GetPointId()
	case *agentpb.BackupCheckpointRequest_MysqlRestoreApplyStart:
		return checkpoint.MysqlRestoreApplyStart.GetPointId()
	case *agentpb.BackupCheckpointRequest_MysqlRestoreVerified:
		return checkpoint.MysqlRestoreVerified.GetPointId()
	default:
		return ""
	}
}
