package backupruntime

import "time"

// CompleteProvenRestoreRecovery uses durable resource receipts, never the
// recovery worker's success flag. It cannot authorize or repeat a mutation.
// The separate work item retains its original failed outcome.
func CompleteProvenRestoreRecovery(current BackupRestoreRecord, at time.Time) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || !current.MutationStarted ||
		(current.State != BackupRestoreVerified && current.State != BackupRestoreRecoveryRequired) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Restore recovery has no completed mutation evidence")
	}
	next := CloneBackupRestoreRecord(current)
	switch current.Point.SourceKind {
	case BackupRuntimeSourceConfig:
		progress := current.ConfigProgress
		if progress == nil || !progress.SourceCleanupCompleted || progress.PublishedHeadRevision <= 0 ||
			progress.MaterializationSHA256 == "" || progress.MaterializationSHA256 != progress.ExpectedMaterializationSHA256 ||
			progress.DeleteEntryOrdinal != progress.DeleteEntryCount || progress.UpsertEntryOrdinal != current.Point.ConfigArchive.EntryCount {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
				"Restore recovery lacks verified configuration publication and cleanup",
			)
		}
	case BackupRuntimeSourceVolume:
		progress := current.VolumeProgress
		if progress == nil || !progress.Exchanged || !progress.OldRemoved || !progress.ServicesRecovered ||
			progress.PendingConstruction != 0 || progress.PendingFinalization != 0 || progress.PendingDeletion != 0 {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
				"Restore recovery lacks completed Volume exchange and consumer recovery",
			)
		}
	case BackupRuntimeSourceAttach:
		progress := current.DatabaseProgress
		if progress != nil && progress.ConsumersOnly {
			if progress.ApplyStarted || progress.RestoreVerificationSHA256 != "" ||
				progress.RecoveryCursor != current.ServiceCount || !progress.SourceCleanupCompleted ||
				!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
				return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
					"consumer-only recovery lacks restored workloads and cleanup",
				)
			}
			next.State, next.Verification, next.UpdatedAt = BackupRestoreFailedSafe, BackupVerificationFailed, at
			return next, ValidateBackupRestoreRecord(next)
		}
		if progress == nil || !progress.ApplyStarted || progress.RestoreVerificationSHA256 == "" ||
			progress.RecoveryCursor != current.ServiceCount || !progress.SourceCleanupCompleted {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord(
				"Restore recovery lacks verified database apply, consumer recovery or cleanup",
			)
		}
	default:
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("Restore recovery source is unsupported")
	}
	// Failure changes the outcome fields, not the receipts checked above.
	// Re-establish only their proven state and run the normal completion rules.
	next.State, next.Verification = BackupRestoreVerified, BackupVerificationPassed
	switch next.Point.SourceKind {
	case BackupRuntimeSourceConfig:
		return CompleteConfigRestore(next, at)
	case BackupRuntimeSourceVolume:
		return CompleteVolumeRestore(next, at)
	default:
		return CompleteDatabaseRestore(next, at)
	}
}
