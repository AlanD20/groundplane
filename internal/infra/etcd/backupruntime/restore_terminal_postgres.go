package backupruntime

import "time"

func CompletePostgresRestore(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceAttach ||
		current.State != BackupRestoreVerified || current.Verification != BackupVerificationPassed ||
		current.PostgresProgress == nil || !current.PostgresProgress.ApplyStarted ||
		current.PostgresProgress.RestoreVerificationSHA256 == "" ||
		current.PostgresProgress.RecoveryCursor != current.ServiceCount ||
		!current.PostgresProgress.SourceCleanupCompleted ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
			"PostgreSQL Restore completion lacks apply proof, consumer recovery, or physical source cleanup")
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.UpdatedAt = BackupRestoreCompleted, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

func FailPostgresRestoreBeforeMutation(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceAttach ||
		current.MutationStarted || current.State == BackupRestoreFailedSafe ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("PostgreSQL Restore failure is not proved safe")
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.Verification, next.UpdatedAt = BackupRestoreFailedSafe, BackupVerificationFailed, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

func RequirePostgresRestoreRecovery(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceAttach ||
		!current.MutationStarted || current.State == BackupRestoreCompleted ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
			"PostgreSQL Restore recovery lacks mutation or apply intent",
		)
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.Verification, next.UpdatedAt = BackupRestoreRecoveryRequired, BackupVerificationFailed, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}
