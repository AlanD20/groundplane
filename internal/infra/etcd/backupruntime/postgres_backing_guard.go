package backupruntime

import "slices"

// DatabaseBackingEnvironmentIDs returns the unique Backing Environments whose
// managed database workloads participate in this Run. The stable order keeps
// transaction construction and replay evidence deterministic.
func DatabaseBackingEnvironmentIDs(record BackupRunRecord) ([]string, error) {
	if ValidateBackupRunRecord(record) != nil {
		return nil, invalidBackupRuntimeRecord("backup run PostgreSQL backing guard authority is invalid")
	}
	unique := make(map[string]struct{})
	for _, source := range record.Sources {
		if source.Kind != BackupRuntimeSourceAttach {
			continue
		}
		environmentID, ok := backupDatabaseBackingEnvironmentID(source)
		if !ok {
			return nil, invalidBackupRuntimeRecord("backup run database backing guard target is missing")
		}
		if environmentID == record.EnvironmentID {
			return nil, invalidBackupRuntimeRecord(
				"backup run PostgreSQL backing guard must be separate from its consumer Environment",
			)
		}
		unique[environmentID] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for environmentID := range unique {
		result = append(result, environmentID)
	}
	slices.Sort(result)
	return result, nil
}

// DatabaseBackingEnvironmentTerminalGuards splits backing Environments into
// guards that terminalization can safely release and guards that must survive
// for startup inventory reconciliation. Only a completed Task proves every
// source helper retired; on an unsuccessful Task every started PostgreSQL
// source remains guarded, including a source whose cleanup checkpoint already
// reached Succeeded. An unstarted source never invoked a helper, and a queued
// Task is known never to have executed.
func DatabaseBackingEnvironmentTerminalGuards(
	record BackupRunRecord,
	neverExecuted bool,
) (release []string, retain []string, err error) {
	environmentIDs, err := DatabaseBackingEnvironmentIDs(record)
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
			environmentID, ok := backupDatabaseBackingEnvironmentID(source)
			if !ok {
				return nil, nil, invalidBackupRuntimeRecord("backup run database backing guard target is missing")
			}
			unsafe[environmentID] = struct{}{}
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

// DatabaseRestoreBackingEnvironmentID returns the managed database Environment
// selected by a database Restore. Other Restore strategies have no such
// guard.
func DatabaseRestoreBackingEnvironmentID(record BackupRestoreRecord) (string, bool, error) {
	if ValidateBackupRestoreRecord(record) != nil {
		return "", false, invalidBackupRuntimeRecord("Restore PostgreSQL backing guard authority is invalid")
	}
	if record.Point.SourceKind != BackupRuntimeSourceAttach {
		return "", false, nil
	}
	var environmentID string
	if record.CurrentTarget.Postgres != nil {
		environmentID = record.CurrentTarget.Postgres.Source.BackingEnvironmentID
	} else if record.CurrentTarget.MySQL != nil {
		environmentID = record.CurrentTarget.MySQL.Source.BackingEnvironmentID
	} else {
		return "", false, invalidBackupRuntimeRecord("Restore database backing guard target is missing")
	}
	if environmentID == record.EnvironmentID {
		return "", false, invalidBackupRuntimeRecord(
			"Restore PostgreSQL backing guard must be separate from its consumer Environment",
		)
	}
	return environmentID, true, nil
}

func backupDatabaseBackingEnvironmentID(source BackupRunSourceAttemptRecord) (string, bool) {
	if source.Snapshot.Postgres != nil && source.Snapshot.MySQL == nil {
		return source.Snapshot.Postgres.BackingEnvironmentID, true
	}
	if source.Snapshot.MySQL != nil && source.Snapshot.Postgres == nil {
		return source.Snapshot.MySQL.BackingEnvironmentID, true
	}
	return "", false
}
