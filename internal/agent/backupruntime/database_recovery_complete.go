package backupruntime

import "github.com/AlanD20/groundplane/proto/agentpb"

func databaseRecoveryComplete(resume *agentpb.BackupRestoreResume, consumers int) bool {
	if resume == nil || resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return false
	}
	if consumers == 0 {
		return resume.GetPostgresRestoreVerified() != nil || resume.GetMysqlRestoreVerified() != nil
	}
	progress := databaseServiceProgress(resume)
	return progress != nil && progress.ServiceCursor == 0 &&
		(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
			progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)
}
