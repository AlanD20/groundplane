package backup

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) ReadRestoreNewVolumeManifest(ctx context.Context, authorization agentregistration.WorkAuthorization,
	wire *agentpb.WorkAttemptIdentity, ordinal uint32,
) (backupvolumemanifest.Complete, error) {
	if _, err := owner.volumeManifestOwner(ctx, authorization, wire, ordinal,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW); err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(wire, authorization.Identity())
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	view, err := authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	restore, _, _, err := owner.inputs.LoadRestoreSource(ctx, view)
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	if restore.Point.SourceKind != backupruntime.BackupRuntimeSourceVolume ||
		restore.State == backupruntime.BackupRestoreFailedSafe || restore.State == backupruntime.BackupRestoreRecoveryRequired {
		return backupvolumemanifest.Complete{}, invalidVolumeManifestAuthority()
	}
	return owner.readPointVolumeManifest(ctx, restore.Point, view.Work.ReadRevision)
}

func (owner *AgentWork) readPointVolumeManifest(ctx context.Context, point backupruntime.BackupRecoveryPointSnapshot,
	revision int64,
) (backupvolumemanifest.Complete, error) {
	if point.SourceKind != backupruntime.BackupRuntimeSourceVolume || backupruntime.ValidateBackupRecoveryPointSnapshot(point) != nil {
		return backupvolumemanifest.Complete{}, invalidVolumeManifestAuthority()
	}
	manifestOwner, err := point.VolumeArchive.Manifest.Owner()
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	complete, err := owner.manifests.ReadComplete(ctx, manifestOwner, revision)
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	archive := point.VolumeArchive
	archive.Manifest = backupruntime.BackupVolumeManifestReference{}
	wire, err := archive.Wire()
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	sourceSHA, err := hex.DecodeString(point.Evidence.SourceSHA256)
	if err != nil {
		return backupvolumemanifest.Complete{}, invalidVolumeManifestAuthority()
	}
	if complete.Revision != point.VolumeArchive.Manifest.CursorRevision || complete.Start.PointId != point.ID ||
		complete.Start.Role != agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED ||
		complete.Start.RestoreGenerationId != "" || complete.Source == nil || complete.Source.SizeBytes != point.Evidence.SourceSizeBytes ||
		!bytes.Equal(complete.Source.SHA256[:], sourceSHA) ||
		!bytes.Equal(wire.ContentManifestSha256, complete.Archive.ContentManifestSHA256[:]) ||
		!bytes.Equal(wire.FullTreeSha256, complete.Archive.FullTreeSHA256[:]) ||
		wire.EntryCount != complete.Archive.EntryCount || wire.SourceSizeBytes != complete.Archive.SourceSizeBytes {
		return backupvolumemanifest.Complete{}, invalidVolumeManifestAuthority()
	}
	return complete, nil
}
