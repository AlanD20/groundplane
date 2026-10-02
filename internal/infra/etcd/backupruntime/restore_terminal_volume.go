package backupruntime

import "time"

func CompleteVolumeRestore(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceVolume ||
		current.State != BackupRestoreVerified || current.Verification != BackupVerificationPassed ||
		current.VolumeProgress == nil || !current.VolumeProgress.OldRemoved ||
		!current.VolumeProgress.ServicesRecovered || !ValidBackupRuntimeInstant(at) ||
		!at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
			"Volume Restore completion lacks exchanged tree cleanup and Service recovery",
		)
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.UpdatedAt = BackupRestoreCompleted, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

func FailVolumeRestoreBeforeMutation(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceVolume ||
		current.MutationStarted || current.State == BackupRestoreFailedSafe ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore failure is not proved safe")
	}
	next := CloneBackupRestoreRecord(current)
	if next.VolumeProgress == nil {
		next.VolumeProgress = &BackupRestoreVolumeProgress{}
	}
	next.State, next.Verification, next.UpdatedAt = BackupRestoreFailedSafe, BackupVerificationFailed, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}

func RequireVolumeRestoreRecovery(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.Point.SourceKind != BackupRuntimeSourceVolume ||
		!current.MutationStarted || current.State == BackupRestoreCompleted ||
		!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Volume Restore recovery lacks mutation intent")
	}
	next := CloneBackupRestoreRecord(current)
	next.State, next.Verification, next.UpdatedAt = BackupRestoreRecoveryRequired, BackupVerificationFailed, at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}
