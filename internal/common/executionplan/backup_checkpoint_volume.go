package executionplan

import (
	"bytes"
	"path"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validBackupVolumeCheckpoint(value *agentpb.BackupVolumeCheckpoint) bool {
	if value == nil {
		return false
	}
	switch checkpoint := value.Checkpoint.(type) {
	case *agentpb.BackupVolumeCheckpoint_ConsumersStopped:
		return checkpoint != nil && checkpoint.ConsumersStopped != nil &&
			backupCheckpointDigest(checkpoint.ConsumersStopped.ServiceProgressSha256)
	case *agentpb.BackupVolumeCheckpoint_ConstructionIntent:
		return checkpoint != nil && validBackupVolumeConstruction(checkpoint.ConstructionIntent)
	case *agentpb.BackupVolumeCheckpoint_ConstructionCompleted:
		if checkpoint == nil || checkpoint.ConstructionCompleted == nil {
			return false
		}
		progress := checkpoint.ConstructionCompleted
		return validBackupVolumeConstruction(progress.Intent) &&
			progress.ConstructionCursorAfter == progress.Intent.ConstructionCursorBefore+1
	case *agentpb.BackupVolumeCheckpoint_FinalizationIntent:
		return checkpoint != nil && validBackupVolumeFinalization(checkpoint.FinalizationIntent)
	case *agentpb.BackupVolumeCheckpoint_FinalizationCompleted:
		if checkpoint == nil || checkpoint.FinalizationCompleted == nil {
			return false
		}
		progress := checkpoint.FinalizationCompleted
		return validBackupVolumeFinalization(progress.Intent) &&
			progress.FinalizationCursorAfter == progress.Intent.FinalizationCursorBefore+1
	case *agentpb.BackupVolumeCheckpoint_ExchangeIntent:
		if checkpoint == nil {
			return false
		}
		progress := checkpoint.ExchangeIntent
		return progress != nil && backupVolumeMutationIdentity(progress.PointId, progress.RestoreGenerationId) &&
			backupVolumeTreePair(progress.OldFullTreeSha256, progress.NewFullTreeSha256)
	case *agentpb.BackupVolumeCheckpoint_TreeExchanged:
		if checkpoint == nil {
			return false
		}
		progress := checkpoint.TreeExchanged
		return progress != nil && backupVolumeMutationIdentity(progress.PointId, progress.RestoreGenerationId) &&
			backupVolumeTreePair(progress.OldFullTreeSha256, progress.NewFullTreeSha256)
	case *agentpb.BackupVolumeCheckpoint_ReplacedPathDeleteIntent:
		return checkpoint != nil && validBackupVolumeDelete(checkpoint.ReplacedPathDeleteIntent)
	case *agentpb.BackupVolumeCheckpoint_VolumeReplacedPathCleaned:
		if checkpoint == nil || checkpoint.VolumeReplacedPathCleaned == nil {
			return false
		}
		progress := checkpoint.VolumeReplacedPathCleaned
		return validBackupVolumeDelete(&agentpb.BackupVolumeDeleteIntent{
			PointId: progress.PointId, RestoreGenerationId: progress.RestoreGenerationId,
			OldFullTreeSha256: progress.OldFullTreeSha256, NewFullTreeSha256: progress.NewFullTreeSha256,
			DeletionCursorBefore: progress.DeletionCursorBefore, DeletionOrdinal: progress.DeletionOrdinal,
			LogicalPath: progress.LogicalPath, Kind: progress.Kind, ParentLogicalPath: progress.ParentLogicalPath,
		})
	case *agentpb.BackupVolumeCheckpoint_ServiceProgress:
		return checkpoint != nil && validBackupVolumeService(checkpoint.ServiceProgress)
	default:
		return false
	}
}

func backupVolumeMutationIdentity(pointID, generationID string) bool {
	return backupCheckpointPoint(pointID) && validRawULID(generationID)
}

func backupVolumeTreePair(oldDigest, newDigest []byte) bool {
	return backupCheckpointDigest(oldDigest) && backupCheckpointDigest(newDigest)
}

func backupVolumeLogicalPath(value []byte) bool {
	if len(value) == 0 || len(value) > backupvolume.MaxPathBytes || bytes.IndexByte(value, 0) >= 0 {
		return false
	}
	if bytes.Equal(value, []byte(".")) {
		return true
	}
	if value[0] == '/' || value[len(value)-1] == '/' {
		return false
	}
	for _, component := range strings.Split(string(value), "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}

func backupVolumeEntryKind(kind agentpb.BackupVolumeEntryKind) bool {
	return kind == agentpb.BackupVolumeEntryKind_BACKUP_VOLUME_ENTRY_KIND_DIRECTORY ||
		kind == agentpb.BackupVolumeEntryKind_BACKUP_VOLUME_ENTRY_KIND_REGULAR_FILE
}

func validBackupVolumeConstruction(value *agentpb.BackupVolumeConstructionIntent) bool {
	if value == nil || !backupVolumeMutationIdentity(value.PointId, value.RestoreGenerationId) ||
		!backupCheckpointDigest(
			value.NewTreeContentManifestSha256,
		) || value.ConstructionCursorBefore >= backupvolume.MaxEntries ||
		value.ArchiveOrdinal == 0 || value.ArchiveOrdinal > backupvolume.MaxEntries ||
		!backupVolumeLogicalPath(value.LogicalPath) || !backupVolumeEntryKind(value.Kind) || value.Mode > 0o7777 {
		return false
	}
	if value.Kind == agentpb.BackupVolumeEntryKind_BACKUP_VOLUME_ENTRY_KIND_DIRECTORY {
		return value.SizeBytes == 0 && backupCheckpointDigest(value.ContentSha256) &&
			bytes.Equal(value.ContentSha256, make([]byte, 32))
	}
	return !bytes.Equal(value.LogicalPath, []byte(".")) && value.SizeBytes <= backupformat.MaxStoredBytes &&
		backupCheckpointDigest(value.ContentSha256)
}

func validBackupVolumeFinalization(value *agentpb.BackupVolumeFinalizationIntent) bool {
	return value != nil && backupVolumeMutationIdentity(value.PointId, value.RestoreGenerationId) &&
		backupCheckpointDigest(
			value.NewTreeContentManifestSha256,
		) && value.FinalizationCursorBefore < backupvolume.MaxEntries &&
		value.FinalizationOrdinal > 0 && value.FinalizationOrdinal <= backupvolume.MaxEntries &&
		backupVolumeLogicalPath(value.LogicalPath) && value.FinalMode <= 0o7777
}

func validBackupVolumeDelete(value *agentpb.BackupVolumeDeleteIntent) bool {
	if value == nil || !backupVolumeMutationIdentity(value.PointId, value.RestoreGenerationId) ||
		!backupVolumeTreePair(
			value.OldFullTreeSha256,
			value.NewFullTreeSha256,
		) || value.DeletionCursorBefore >= backupvolume.MaxEntries ||
		value.DeletionOrdinal == 0 || value.DeletionOrdinal > backupvolume.MaxEntries ||
		!backupVolumeLogicalPath(value.LogicalPath) || !backupVolumeEntryKind(value.Kind) {
		return false
	}
	if bytes.Equal(value.LogicalPath, []byte(".")) {
		return value.Kind == agentpb.BackupVolumeEntryKind_BACKUP_VOLUME_ENTRY_KIND_DIRECTORY &&
			len(value.ParentLogicalPath) == 0
	}
	return backupVolumeLogicalPath(value.ParentLogicalPath) &&
		string(value.ParentLogicalPath) == path.Dir(string(value.LogicalPath))
}

func validBackupVolumeService(value *agentpb.BackupVolumeServiceProgress) bool {
	return value != nil && backupCheckpointServiceID(value.ServiceId) &&
		backupCheckpointServicePhase(value.Phase) && backupCheckpointDigest(value.ObservationSha256)
}
