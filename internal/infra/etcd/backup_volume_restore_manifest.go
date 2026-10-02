package etcd

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *BackupRuntimeRepository) validateVolumeRestoreSelectedManifest(ctx context.Context,
	point backupruntime.BackupRecoveryPointSnapshot,
) error {
	if repository == nil || repository.store == nil ||
		backupruntime.ValidateBackupRecoveryPointSnapshot(point) != nil ||
		point.SourceKind != backupruntime.BackupRuntimeSourceVolume {
		return errs.New(errs.KindValidationFailed, "Volume Restore Point manifest authority is invalid")
	}
	owner, err := point.VolumeArchive.Manifest.Owner()
	if err != nil {
		return err
	}
	ledger := backupvolumemanifest.NewRepository(repository.store, nil)
	complete, err := ledger.ReadComplete(ctx, owner, 0)
	if err != nil {
		return err
	}
	archive := point.VolumeArchive
	archive.Manifest = backupruntime.BackupVolumeManifestReference{}
	wire, err := archive.Wire()
	sourceSHA, shaErr := hex.DecodeString(point.Evidence.SourceSHA256)
	if err != nil || shaErr != nil || complete.Revision != point.VolumeArchive.Manifest.CursorRevision ||
		complete.Start.PointId != point.ID || complete.Start.RestoreGenerationId != "" ||
		complete.Start.Role != agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED ||
		complete.Source == nil || complete.Source.SizeBytes != point.Evidence.SourceSizeBytes ||
		!bytes.Equal(complete.Source.SHA256[:], sourceSHA) ||
		wire.EntryCount != complete.Archive.EntryCount || wire.SourceSizeBytes != complete.Archive.SourceSizeBytes ||
		!bytes.Equal(wire.ContentManifestSha256, complete.Archive.ContentManifestSHA256[:]) ||
		!bytes.Equal(wire.FullTreeSha256, complete.Archive.FullTreeSHA256[:]) {
		return errs.New(errs.KindStateConflict, "Volume Restore Point manifest differs from its retained capture")
	}
	return nil
}
