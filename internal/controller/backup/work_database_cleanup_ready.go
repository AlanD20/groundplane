package backup

import "github.com/AlanD20/groundplane/proto/agentpb"

func databaseFilesCleanupReady(resume *agentpb.BackupRestoreResume, consumers int) bool {
	if resume.GetSourceCleanupCompleted() != nil {
		return true
	}
	if resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return false
	}
	if consumers == 0 {
		return resume.GetPostgresRestoreVerified() != nil || resume.GetMysqlRestoreVerified() != nil
	}
	progress := resume.GetPostgresServiceProgress()
	if progress == nil {
		if mysql := resume.GetMysqlServiceProgress(); mysql != nil {
			progress = &agentpb.BackupPostgresServiceProgress{ServiceCursor: mysql.ServiceCursor,
				ServiceId: mysql.ServiceId, Phase: mysql.Phase, ObservationSha256: mysql.ObservationSha256}
		}
	}
	return progress != nil && progress.ServiceCursor == 0 &&
		(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
			progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)
}
