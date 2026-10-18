package backup

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) prepareVolumeRestoreProgress(ctx context.Context,
	view workqueue.ResumeView, admitted backupwork.CheckpointAdmission, at time.Time,
) (workqueue.Change, error) {
	ordinal := admitted.Request.SourceOrdinal
	step := admitted.Input().Steps[ordinal]
	current, guard, err := owner.inputs.LoadStagingRestore(ctx, backupwork.StagingSource{
		View: view.Work, Input: admitted.Input(), Sources: admitted.Sources(), Ordinal: ordinal,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	newManifest, err := owner.readPointVolumeManifest(ctx, current.Point, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions := []keyvalue.Condition{{
		Key:         backupvolumemanifest.CursorKey(current.Point.VolumeArchive.Manifest.Binding),
		ModRevision: newManifest.Revision,
	}}
	var oldManifest *backupvolumemanifest.Complete
	checkpoint := admitted.Request.GetVolume()
	if checkpoint.GetConsumersStopped() != nil || checkpoint.GetReplacedPathDeleteIntent() != nil ||
		checkpoint.GetVolumeReplacedPathCleaned() != nil {
		var binding backupvolumetransfer.Binding
		if admitted.Recovery != nil {
			binding, err = backupvolumetransfer.BindRecoverySource(admitted.Recovery, view.Attempt.Attempt.Executor, ordinal,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		} else {
			binding, err = backupvolumetransfer.Bind(admitted.Assignment, view.Attempt.Attempt.Executor, ordinal,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		}
		if err != nil {
			return workqueue.Change{}, err
		}
		old, err := owner.manifests.ReadComplete(ctx, backupvolumemanifest.Owner{Binding: binding}, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if old.Start == nil || old.Start.PointId != step.GetRestore().PointId ||
			old.Start.RestoreGenerationId != step.ExecutionId ||
			old.Start.Role != agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_STOPPED_LIVE_OLD || old.Source != nil {
			return workqueue.Change{}, invalidVolumeManifestAuthority()
		}
		oldManifest = &old
		conditions = append(conditions, keyvalue.Condition{Key: backupvolumemanifest.CursorKey(binding), ModRevision: old.Revision})
	}
	if err := validateVolumeRestoreCheckpointEntries(checkpoint, newManifest, oldManifest); err != nil {
		return workqueue.Change{}, err
	}
	var proof *backupruntime.BackupVolumeOldManifestProof
	if oldManifest != nil && checkpoint.GetConsumersStopped() != nil {
		proof = &backupruntime.BackupVolumeOldManifestProof{EntryCount: oldManifest.Archive.EntryCount,
			ContentManifestSHA256: oldManifest.Archive.ContentManifestSHA256, FullTreeSHA256: oldManifest.Archive.FullTreeSHA256}
	}
	if !at.After(current.UpdatedAt) {
		at = current.UpdatedAt.Add(time.Nanosecond)
	}
	change, err := owner.configRuntime.PrepareVolumeRestoreProgress(ctx, view, admitted,
		keyvalue.Versioned[backupruntime.BackupRestoreRecord]{Record: current, Revision: guard.ModRevision, ReadRevision: view.Work.ReadRevision},
		proof, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, conditions...)
	return change, nil
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
