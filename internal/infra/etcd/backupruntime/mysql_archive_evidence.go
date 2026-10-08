package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/backupmysql"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type BackupMySQLArchiveEvidence = backupmysql.ArchiveEvidence

func MySQLArchiveEvidenceFromWire(value *agentpb.BackupMySQLArchiveEvidence) (BackupMySQLArchiveEvidence, error) {
	return backupmysql.FromWire(value)
}

func validateSelectedMySQLArchive(kind BackupRuntimeSourceKind, format BackupRuntimeFormat,
	archive BackupMySQLArchiveEvidence,
) error {
	if format != BackupRuntimeFormatMySQL {
		if archive != (BackupMySQLArchiveEvidence{}) {
			return invalidBackupRuntimeRecord("non-MySQL source carries MySQL archive evidence")
		}
		return nil
	}
	if kind != BackupRuntimeSourceAttach || archive.Validate() != nil {
		return invalidBackupRuntimeRecord("MySQL source archive evidence is invalid")
	}
	return nil
}
