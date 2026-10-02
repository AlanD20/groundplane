package agent

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func backupRecoveredFileRole(name string) agentpb.BackupRecoveredFileRole {
	switch name {
	case executionplan.BackupSourceStagingFinal, "." + executionplan.BackupSourceStagingFinal + ".partial":
		return agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT
	case executionplan.BackupStoredStagingFinal, "." + executionplan.BackupStoredStagingFinal + ".partial":
		return agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT
	default:
		return agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_UNSPECIFIED
	}
}
