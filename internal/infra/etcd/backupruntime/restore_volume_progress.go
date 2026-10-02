package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func validateBackupRestoreVolumeProgress(record BackupRestoreRecord) error {
	progress := record.VolumeProgress
	if record.State == BackupRestoreQueued || record.State == BackupRestoreDownloading {
		if progress != nil {
			return invalidBackupRuntimeRecord("Volume Restore has progress before artifact verification")
		}
		return nil
	}
	if progress == nil || progress.ConstructionCursor > record.Point.VolumeArchive.EntryCount ||
		progress.FinalizationCursor > progress.ConstructionCursor ||
		progress.ServiceCursor > record.ServiceCount ||
		progress.PendingConstruction > record.Point.VolumeArchive.EntryCount ||
		progress.PendingFinalization > record.Point.VolumeArchive.EntryCount ||
		progress.PendingConstruction != 0 && progress.PendingConstruction != progress.ConstructionCursor+1 ||
		progress.PendingFinalization != 0 && progress.PendingFinalization != progress.FinalizationCursor+1 ||
		progress.ExchangeIntent && progress.FinalizationCursor != record.Point.VolumeArchive.EntryCount ||
		progress.Exchanged && !progress.ExchangeIntent ||
		progress.OldRemoved && (!progress.Exchanged || progress.DeletionCursor != progress.OldEntryCount) ||
		progress.ServicesRecovered && (!progress.OldRemoved || progress.ServiceCursor != record.ServiceCount) {
		return invalidBackupRuntimeRecord("Volume Restore progress is inconsistent")
	}
	if progress.ServiceMutationStarted != record.MutationStarted &&
		record.State != BackupRestoreExchangeReady && record.State != BackupRestoreExchanged &&
		record.State != BackupRestoreConsumersRestored && record.State != BackupRestoreVerified &&
		record.State != BackupRestoreCompleted && record.State != BackupRestoreRecoveryRequired {
		return invalidBackupRuntimeRecord("Volume Restore mutation intent differs from native state")
	}
	if progress.ConsumersStopped {
		if progress.OldEntryCount < 1 || progress.OldEntryCount > backupvolume.MaxEntries ||
			!recordcodec.ValidSHA256(progress.OldContentManifestSHA256) ||
			!recordcodec.ValidSHA256(progress.OldFullTreeSHA256) ||
			progress.DeletionCursor > progress.OldEntryCount ||
			progress.PendingDeletion > progress.OldEntryCount ||
			progress.PendingDeletion != 0 && progress.PendingDeletion != progress.DeletionCursor+1 {
			return invalidBackupRuntimeRecord("Volume Restore old-tree progress is invalid")
		}
	} else if progress.OldEntryCount != 0 || progress.OldContentManifestSHA256 != "" ||
		progress.OldFullTreeSHA256 != "" || progress.ConstructionCursor != 0 ||
		progress.FinalizationCursor != 0 || progress.DeletionCursor != 0 ||
		progress.PendingConstruction != 0 || progress.PendingFinalization != 0 ||
		progress.PendingDeletion != 0 || progress.ExchangeIntent || progress.Exchanged || progress.OldRemoved {
		return invalidBackupRuntimeRecord("Volume Restore mutation precedes stopped consumers")
	}
	switch record.State {
	case BackupRestoreArtifactVerified:
		if progress.ConsumersStopped || progress.ServiceMutationStarted {
			return invalidBackupRuntimeRecord("Volume Restore stopped state was not published")
		}
	case BackupRestoreConsumersStopped:
		if !progress.ServiceMutationStarted || progress.ExchangeIntent {
			return invalidBackupRuntimeRecord("Volume Restore Service stop intent is missing")
		}
	case BackupRestoreRestoring:
		if !progress.ConsumersStopped || progress.ExchangeIntent {
			return invalidBackupRuntimeRecord("Volume Restore construction state is invalid")
		}
	case BackupRestoreTreeValidated:
		if progress.FinalizationCursor != record.Point.VolumeArchive.EntryCount || progress.ExchangeIntent {
			return invalidBackupRuntimeRecord("Volume Restore hidden tree lacks finalization")
		}
	case BackupRestoreExchangeReady:
		if !progress.ExchangeIntent || progress.Exchanged {
			return invalidBackupRuntimeRecord("Volume Restore exchange intent is missing")
		}
	case BackupRestoreExchanged, BackupRestoreConsumersRestored, BackupRestoreVerified, BackupRestoreCompleted:
		if !progress.Exchanged {
			return invalidBackupRuntimeRecord("Volume Restore has not exchanged trees")
		}
		if record.State == BackupRestoreVerified || record.State == BackupRestoreCompleted {
			if !progress.OldRemoved || !progress.ServicesRecovered {
				return invalidBackupRuntimeRecord("Volume Restore lacks cleanup or Service recovery")
			}
		}
	case BackupRestoreFailedSafe, BackupRestoreRecoveryRequired:
		// Retain the last exact cursor, including any pending local mutation.
	default:
		return invalidBackupRuntimeRecord("Volume Restore progress state is unsupported")
	}
	return nil
}
