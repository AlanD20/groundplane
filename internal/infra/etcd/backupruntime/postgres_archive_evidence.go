package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/backuppostgres"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type BackupPostgresArchiveEvidence = backuppostgres.ArchiveEvidence

func PostgresArchiveEvidenceFromWire(value *agentpb.BackupPostgresArchiveEvidence) (
	BackupPostgresArchiveEvidence, error,
) {
	return backuppostgres.FromWire(value)
}

func validateSelectedPostgresArchive(kind BackupRuntimeSourceKind, format BackupRuntimeFormat,
	archive BackupPostgresArchiveEvidence,
) error {
	if format != BackupRuntimeFormatPostgres {
		if archive != (BackupPostgresArchiveEvidence{}) {
			return invalidBackupRuntimeRecord("non-PostgreSQL source carries PostgreSQL archive evidence")
		}
		return nil
	}
	if kind != BackupRuntimeSourceAttach || archive.Validate() != nil {
		return invalidBackupRuntimeRecord("PostgreSQL source archive evidence is invalid")
	}
	return nil
}
