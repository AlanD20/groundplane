package backup

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *BackupCheckpointService) checkpointVolumeRestore(ctx context.Context,
	input backupruntime.BackupCheckpointInput, step *agentpb.BackupStepAuthority, replayRevision int64,
) (*agentpb.BackupCheckpointAck, error) {
	if step.GetRestore().GetVolume() == nil {
		return nil, invalidVolumeManifestAuthority()
	}
	current, err := service.repository.GetBackupRestore(ctx, input.TaskID)
	if err != nil {
		return nil, err
	}
	newManifest, err := service.ReadPointVolumeManifest(ctx, current.Record.Point)
	if err != nil {
		return nil, err
	}
	var oldManifest *backupvolumemanifest.Complete
	if input.Request.GetVolume() != nil && input.Request.GetVolume().GetConsumersStopped() != nil ||
		input.Request.GetVolume().GetReplacedPathDeleteIntent() != nil ||
		input.Request.GetVolume().GetVolumeReplacedPathCleaned() != nil {
		old, err := service.ReadCompleteVolumeManifest(ctx, input.AgentID, input.AgentGeneration,
			input.TaskID, input.AssignmentID, input.StepID,
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD, 0)
		if err != nil || old.Start == nil || old.Start.PointId != step.GetRestore().PointId ||
			old.Start.RestoreGenerationId != step.ExecutionId || old.Start.Role !=
			agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_STOPPED_LIVE_OLD || old.Source != nil {
			return nil, invalidVolumeManifestAuthority()
		}
		oldManifest = &old
	}
	if err := validateVolumeRestoreCheckpointEntries(input.Request.GetVolume(), newManifest, oldManifest); err != nil {
		return nil, err
	}
	if replayRevision > 0 {
		return backupCheckpointAcknowledgement(input.Request, replayRevision)
	}
	var proof *backupruntime.BackupVolumeOldManifestProof
	if oldManifest != nil && input.Request.GetVolume().GetConsumersStopped() != nil {
		proof = &backupruntime.BackupVolumeOldManifestProof{EntryCount: oldManifest.Archive.EntryCount,
			ContentManifestSHA256: oldManifest.Archive.ContentManifestSHA256,
			FullTreeSHA256:        oldManifest.Archive.FullTreeSHA256}
	}
	at := service.now().UTC()
	if !at.After(current.Record.UpdatedAt) {
		at = current.Record.UpdatedAt.Add(time.Nanosecond)
	}
	revision, err := service.repository.CheckpointVolumeRestore(ctx, input, current, proof, at)
	if err != nil {
		return nil, err
	}
	return backupCheckpointAcknowledgement(input.Request, revision)
}

func validateVolumeRestoreCheckpointEntries(checkpoint *agentpb.BackupVolumeCheckpoint,
	newManifest backupvolumemanifest.Complete, oldManifest *backupvolumemanifest.Complete,
) error {
	if checkpoint == nil {
		return nil
	}
	match := func(entry backupvolume.Entry, path []byte, kind agentpb.BackupVolumeEntryKind,
		mode, uid, gid uint32, size uint64, digest []byte,
	) bool {
		return bytes.Equal(entry.Path, path) && agentpb.BackupVolumeEntryKind(entry.Kind) == kind &&
			entry.Mode == mode && entry.UID == uid && entry.GID == gid && entry.SizeBytes == size &&
			bytes.Equal(entry.ContentSHA256[:], digest)
	}
	if intent := checkpoint.GetConstructionIntent(); intent != nil {
		if intent.ArchiveOrdinal == 0 || intent.ArchiveOrdinal > uint64(len(newManifest.Entries)) ||
			!match(newManifest.Entries[intent.ArchiveOrdinal-1], intent.LogicalPath, intent.Kind,
				intent.Mode, intent.Uid, intent.Gid, intent.SizeBytes, intent.ContentSha256) {
			return invalidVolumeManifestAuthority()
		}
	}
	if completed := checkpoint.GetConstructionCompleted(); completed != nil {
		intent := completed.Intent
		if intent == nil || intent.ArchiveOrdinal == 0 || intent.ArchiveOrdinal > uint64(len(newManifest.Entries)) ||
			!match(newManifest.Entries[intent.ArchiveOrdinal-1], intent.LogicalPath, intent.Kind,
				intent.Mode, intent.Uid, intent.Gid, intent.SizeBytes, intent.ContentSha256) {
			return invalidVolumeManifestAuthority()
		}
	}
	if intent := checkpoint.GetFinalizationIntent(); intent != nil {
		order, err := backupvolumefs.FinalizationOrder(newManifest.Entries)
		if err != nil || intent.FinalizationOrdinal == 0 || intent.FinalizationOrdinal > uint64(len(order)) {
			return invalidVolumeManifestAuthority()
		}
		entry := order[intent.FinalizationOrdinal-1]
		if !bytes.Equal(entry.Path, intent.LogicalPath) || entry.Mode != intent.FinalMode ||
			entry.UID != intent.FinalUid || entry.GID != intent.FinalGid {
			return invalidVolumeManifestAuthority()
		}
	}
	if completed := checkpoint.GetFinalizationCompleted(); completed != nil {
		return validateVolumeRestoreCheckpointEntries(&agentpb.BackupVolumeCheckpoint{
			Checkpoint: &agentpb.BackupVolumeCheckpoint_FinalizationIntent{FinalizationIntent: completed.Intent}},
			newManifest, oldManifest)
	}
	if intent := checkpoint.GetReplacedPathDeleteIntent(); intent != nil {
		if oldManifest == nil || !volumeDeleteEntryMatches(*oldManifest, intent.DeletionOrdinal,
			intent.LogicalPath, intent.Kind) {
			return invalidVolumeManifestAuthority()
		}
	}
	if completed := checkpoint.GetVolumeReplacedPathCleaned(); completed != nil {
		if oldManifest == nil || !volumeDeleteEntryMatches(*oldManifest, completed.DeletionOrdinal,
			completed.LogicalPath, completed.Kind) {
			return invalidVolumeManifestAuthority()
		}
	}
	return nil
}

func volumeDeleteEntryMatches(old backupvolumemanifest.Complete, ordinal uint64,
	path []byte, kind agentpb.BackupVolumeEntryKind,
) bool {
	tree, err := backupvolumefs.TreeFromEntries(old.Entries)
	if err != nil {
		return false
	}
	order, err := backupvolumefs.DeletionOrder(tree)
	if err != nil || ordinal == 0 || ordinal > uint64(len(order)) {
		return false
	}
	entry := order[ordinal-1]
	return bytes.Equal(entry.Path, path) && agentpb.BackupVolumeEntryKind(entry.Kind) == kind
}
