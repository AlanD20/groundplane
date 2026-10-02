package backupruntime

import "time"

// Successful settlement is deliberately stricter than Agent Task completion:
// canonical publication, file verification and physical source cleanup must
// already have durable native receipts. Mutation failures require recovery.
func CompleteConfigRestore(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceConfig ||
		current.State != BackupRestoreVerified || current.Verification != BackupVerificationPassed ||
		current.ConfigProgress == nil || !current.ConfigProgress.SourceCleanupCompleted ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
			"Restore completion requires verified files and source cleanup",
		)
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.UpdatedAt = BackupRestoreCompleted, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

// A safe failure is permitted only before the durable live-mutation intent.
// In particular, a partially published generation cannot release its lock.
func FailConfigRestoreBeforeMutation(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceConfig ||
		current.MutationStarted || current.State == BackupRestoreFailedSafe ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Restore failure is not proved safe")
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.Verification, next.UpdatedAt = BackupRestoreFailedSafe, BackupVerificationFailed, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

// Recovery retains the exact publication cursor and all verification evidence.
// It does not restore latest desired state or authorize another Restore attempt.
func RequireConfigRestoreRecovery(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceConfig ||
		!current.MutationStarted || current.State == BackupRestoreCompleted ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Restore recovery lacks live mutation authority")
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.Verification, next.UpdatedAt = BackupRestoreRecoveryRequired, BackupVerificationFailed, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

func BackupRestoreTerminalDomainDigest(record BackupRestoreRecord) (string, error) {
	if err := ValidateBackupRestoreRecord(record); err != nil {
		return "", err
	}
	return backupTerminalCanonicalDigest("groundplane.backup.terminal.restore.v1\x00", record)
}
