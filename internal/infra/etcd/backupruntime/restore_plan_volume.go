package backupruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidateVolumeRestoreExecutionPlan(record BackupRestoreRecord, plan *agentpb.ExecutionPlan) error {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return err
	}
	if ValidateBackupRestoreRecord(record) != nil || record.Point.SourceKind != BackupRuntimeSourceVolume ||
		validated.Operation != agentpb.PlanOperation_PLAN_OPERATION_RESTORE ||
		validated.TargetId != record.EnvironmentID || len(validated.Steps) != 1 || validated.BackupScope == nil ||
		len(validated.Artifacts) == 0 {
		return invalidBackupRuntimeRecord("Volume Restore execution plan is invalid")
	}
	target, scope := record.CurrentTarget.Volume, validated.BackupScope
	step := validated.Steps[0].GetBackupStep()
	restore := step.GetRestore()
	volume := restore.GetVolume()
	if target == nil || scope.EnvironmentId != record.EnvironmentID ||
		scope.Environment.GetModRevision() != target.EnvironmentRevision ||
		restore == nil || volume == nil || restore.PointId != record.Point.ID ||
		restore.Destination.GetKind() != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME ||
		restore.Destination.GetResourceId() != record.Point.TargetID ||
		restore.Destination.GetResource().GetModRevision() != target.HeadRevision ||
		volume.VolumeId != record.Point.TargetID ||
		!proto.Equal(volume.Volume, restore.Destination.Resource) ||
		step.ExecutionId != record.RestoreGenerationID ||
		!backupObjectMatchesWire(record.Point.Object, restore.SourceObject) ||
		restore.SourceObject.GetConnector().GetConnector().GetModRevision() != record.ConnectorRevision ||
		!BackupArtifactEvidenceMatchesWire(record.Point.Evidence, restore.ExpectedEvidence) ||
		step.StepDeadlineUnixNano != uint64(record.CreatedAt.Add(6*time.Hour).UnixNano()) ||
		len(step.ConsumerServiceIds) != len(target.Services) {
		return invalidBackupRuntimeRecord("Volume Restore selected authority changed")
	}
	headSHA, err := hex.DecodeString(target.HeadSHA256)
	if err != nil || !bytes.Equal(headSHA, restore.Destination.Resource.Sha256) {
		return invalidBackupRuntimeRecord("Volume Restore destination head changed")
	}
	archive, err := record.Point.VolumeArchive.Wire()
	if err != nil || !proto.Equal(archive, volume.Archive) {
		return invalidBackupRuntimeRecord("Volume Restore archive authority changed")
	}
	projection := volume.Projection
	artifactSHA, err := hex.DecodeString(target.ArtifactDigest)
	if err != nil || projection == nil || projection.ArtifactId != target.ArtifactID ||
		!bytes.Equal(projection.ArtifactSha256, artifactSHA) ||
		projection.ArtifactRevision != target.ArtifactRevision || projection.ProjectionRoot != target.ProjectionRoot ||
		projection.RenderGeneration != target.RenderGeneration ||
		projection.ComposeVolumeKey != target.ComposeVolumeKey ||
		projection.DockerVolumeName != target.DockerVolumeName ||
		projection.AuthorizedVolumeDir != target.AuthorizedVolumeDir {
		return invalidBackupRuntimeRecord("Volume Restore projection authority changed")
	}
	for index, consumer := range target.Services {
		if step.ConsumerServiceIds[index] != consumer.ServiceID {
			return invalidBackupRuntimeRecord("Volume Restore consumer set changed")
		}
	}
	encryption := restore.Encryption
	switch record.Point.Encryption {
	case BackupRuntimeEncryptionNone:
		if encryption.GetKind() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE ||
			encryption.SecretSlotId != "" || encryption.KeyEra != nil {
			return invalidBackupRuntimeRecord("Volume Restore unencrypted authority changed")
		}
	case BackupRuntimeEncryptionAge:
		recipient := sha256.Sum256([]byte(record.Point.Recipient))
		if encryption.GetKind() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE ||
			encryption.GetKeyEra() != uint64(record.Point.KeyEra) ||
			!bytes.Equal(encryption.RecipientSha256, recipient[:]) {
			return invalidBackupRuntimeRecord("Volume Restore age authority changed")
		}
		if record.UsesOldIdentity {
			if encryption.SecretSlotId != backupsecret.OperatorOldAgeIdentitySlotID {
				return invalidBackupRuntimeRecord("Volume Restore old identity slot changed")
			}
		} else if encryption.SecretSlotId != backupsecret.CurrentAgeIdentitySlotID ||
			encryption.SecretSlot.GetModRevision() != record.ExpectedKeyValueRevision {
			return invalidBackupRuntimeRecord("Volume Restore current identity slot changed")
		}
	default:
		return invalidBackupRuntimeRecord("Volume Restore encryption strategy changed")
	}
	return nil
}
