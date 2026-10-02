package backupruntime

import "slices"

// PostgresBackingEnvironmentIDs returns the unique Backing Environments whose
// managed PostgreSQL workloads participate in this Run. The stable order keeps
// transaction construction and replay evidence deterministic.
func PostgresBackingEnvironmentIDs(record BackupRunRecord) ([]string, error) {
	if ValidateBackupRunRecord(record) != nil {
		return nil, invalidBackupRuntimeRecord("backup run PostgreSQL backing guard authority is invalid")
	}
	unique := make(map[string]struct{})
	for _, source := range record.Sources {
		if source.Kind != BackupRuntimeSourceAttach {
			continue
		}
		if source.Snapshot.Postgres == nil {
			return nil, invalidBackupRuntimeRecord("backup run PostgreSQL backing guard target is missing")
		}
		if source.Snapshot.Postgres.BackingEnvironmentID == record.EnvironmentID {
			return nil, invalidBackupRuntimeRecord(
				"backup run PostgreSQL backing guard must be separate from its consumer Environment",
			)
		}
		unique[source.Snapshot.Postgres.BackingEnvironmentID] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for environmentID := range unique {
		result = append(result, environmentID)
	}
	slices.Sort(result)
	return result, nil
}

// PostgresBackingEnvironmentTerminalGuards splits backing Environments into
// guards that terminalization can safely release and guards that must survive
// for startup inventory reconciliation. Only a completed Task proves every
// source helper retired; on an unsuccessful Task every started PostgreSQL
// source remains guarded, including a source whose cleanup checkpoint already
// reached Succeeded. An unstarted source never invoked a helper, and a queued
// Task is known never to have executed.
func PostgresBackingEnvironmentTerminalGuards(
	record BackupRunRecord,
	neverExecuted bool,
) (release []string, retain []string, err error) {
	environmentIDs, err := PostgresBackingEnvironmentIDs(record)
	if err != nil {
		return nil, nil, err
	}
	unsafe := make(map[string]struct{})
	if !neverExecuted && record.State != BackupRunCompleted {
		for _, source := range record.Sources {
			if source.Kind != BackupRuntimeSourceAttach ||
				source.State == BackupSourceAttemptUnstarted {
				continue
			}
			unsafe[source.Snapshot.Postgres.BackingEnvironmentID] = struct{}{}
		}
	}
	for _, environmentID := range environmentIDs {
		if _, found := unsafe[environmentID]; found {
			retain = append(retain, environmentID)
		} else {
			release = append(release, environmentID)
		}
	}
	return release, retain, nil
}

// PostgresRestoreBackingEnvironmentID returns the managed database Environment
// selected by a PostgreSQL Restore. Other Restore strategies have no such
// guard.
func PostgresRestoreBackingEnvironmentID(record BackupRestoreRecord) (string, bool, error) {
	if ValidateBackupRestoreRecord(record) != nil {
		return "", false, invalidBackupRuntimeRecord("Restore PostgreSQL backing guard authority is invalid")
	}
	if record.Point.SourceKind != BackupRuntimeSourceAttach {
		return "", false, nil
	}
	if record.CurrentTarget.Postgres == nil {
		return "", false, invalidBackupRuntimeRecord("Restore PostgreSQL backing guard target is missing")
	}
	if record.CurrentTarget.Postgres.Source.BackingEnvironmentID == record.EnvironmentID {
		return "", false, invalidBackupRuntimeRecord(
			"Restore PostgreSQL backing guard must be separate from its consumer Environment",
		)
	}
	return record.CurrentTarget.Postgres.Source.BackingEnvironmentID, true, nil
}
