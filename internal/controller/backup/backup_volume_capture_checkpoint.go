package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func volumeManifestReference(
	cursor etcdstore.Versioned[backupvolumemanifest.Cursor],
) backupruntime.BackupVolumeManifestReference {
	owner := cursor.Record.Owner
	return backupruntime.BackupVolumeManifestReference{TaskID: owner.Binding.TaskID,
		AssignmentID: owner.Binding.AssignmentID, StepID: owner.Binding.StepID,
		TransferID: owner.Binding.TransferID, AuthoritySHA256: hex.EncodeToString(owner.Binding.AuthorityDigest[:]),
		AgentID: owner.AgentID, AgentGeneration: owner.AgentGeneration,
		AssignmentGeneration: owner.AssignmentGeneration, CursorRevision: cursor.Revision}
}

func (service *BackupCheckpointService) validateVolumeCaptureCompletion(
	ctx context.Context,
	input backupruntime.BackupCheckpointInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	ordinal int,
) (etcdstore.Versioned[backupvolumemanifest.Cursor], error) {
	var zero etcdstore.Versioned[backupvolumemanifest.Cursor]
	prepared := input.Request.GetArtifactPrepared()
	if prepared == nil || prepared.GetVolume() == nil || ordinal < 0 || ordinal >= len(current.Record.Sources) ||
		current.Record.Sources[ordinal].Kind != backupruntime.BackupRuntimeSourceVolume {
		return zero, invalidVolumeManifestAuthority()
	}
	owner, err := service.volumeManifestOwner(ctx, input.AgentID, input.AgentGeneration,
		input.TaskID, input.AssignmentID, input.StepID,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE)
	if err != nil {
		return zero, err
	}
	ledger := service.repository.VolumeManifestRepository()
	cursor, found, err := ledger.Read(ctx, owner, 0)
	if err != nil || !found || !cursor.Record.Complete {
		return zero, invalidVolumeManifestAuthority()
	}
	complete, err := ledger.ReadComplete(ctx, owner, cursor.ReadRevision)
	if err != nil {
		return zero, err
	}
	archive, err := backupvolumetransfer.ArchiveEvidenceFromWire(prepared.GetVolume())
	if err != nil || archive != complete.Archive || complete.Source == nil ||
		complete.Source.SizeBytes != prepared.Evidence.SourceSizeBytes ||
		!bytes.Equal(complete.Source.SHA256[:], prepared.Evidence.SourceSha256) ||
		len(prepared.Evidence.SourceSha256) != sha256.Size ||
		complete.Start.Role != agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED ||
		complete.Start.PointId != current.Record.Sources[ordinal].RecoveryPointID ||
		complete.Start.RestoreGenerationId != "" {
		return zero, errs.New(errs.KindValidationFailed, "Volume artifact differs from its committed capture manifest")
	}
	return cursor, nil
}
