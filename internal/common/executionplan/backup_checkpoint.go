package executionplan

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateBackupCheckpointRequest validates one private delivery against the
// exact next sequence held by the future durable checkpoint cursor. The ADR
// does not define a canonical payload serialization, so this boundary validates
// the protected digest shape but deliberately does not invent a digest input.
func ValidateBackupCheckpointRequest(
	request *agentpb.BackupCheckpointRequest,
	expectedSequence uint32,
) (*agentpb.BackupCheckpointRequest, error) {
	if request == nil || expectedSequence == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint request or sequence is invalid")
	}
	owned := proto.Clone(request).(*agentpb.BackupCheckpointRequest)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTask, owned.TaskId) != nil || ids.Validate(ids.KindAssignment, owned.AssignmentId) != nil ||
		ids.Validate(ids.KindStep, owned.StepId) != nil || owned.Sequence != expectedSequence ||
		len(owned.ControlPayloadSha256) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint delivery identity is invalid")
	}
	if err := validateBackupCheckpointPayload(owned); err != nil {
		return nil, err
	}
	return owned, nil
}

// ValidateBackupCheckpointAck validates the identity-only acknowledgement and
// requires it to echo the exact request delivery tuple.
func ValidateBackupCheckpointAck(
	ack *agentpb.BackupCheckpointAck,
	request *agentpb.BackupCheckpointRequest,
) (*agentpb.BackupCheckpointAck, error) {
	if ack == nil || request == nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint acknowledgement is invalid")
	}
	owned := proto.Clone(ack).(*agentpb.BackupCheckpointAck)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTask, owned.TaskId) != nil || ids.Validate(ids.KindAssignment, owned.AssignmentId) != nil ||
		ids.Validate(ids.KindStep, owned.StepId) != nil || owned.Sequence == 0 || owned.TaskId != request.TaskId ||
		owned.AssignmentId != request.AssignmentId || owned.StepId != request.StepId ||
		owned.Sequence != request.Sequence {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint acknowledgement identity is invalid")
	}
	return owned, nil
}

func validateBackupCheckpointPayload(request *agentpb.BackupCheckpointRequest) error {
	validPoint := func(pointID string) bool { return ids.Validate(ids.KindRecoveryPoint, pointID) == nil }
	validDigest := func(digest []byte) bool { return len(digest) == sha256.Size }
	validStored := func(pointID string, size uint64, digest []byte) bool {
		return validPoint(pointID) && size > 0 && validDigest(digest)
	}
	valid := false
	switch payload := request.Payload.(type) {
	case *agentpb.BackupCheckpointRequest_ArtifactPrepared:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED &&
			payload.ArtifactPrepared != nil && validStored(
			payload.ArtifactPrepared.PointId,
			payload.ArtifactPrepared.StoredSizeBytes,
			payload.ArtifactPrepared.StoredSha256,
		)
	case *agentpb.BackupCheckpointRequest_UploadVerified:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_VERIFIED &&
			payload.UploadVerified != nil && validStored(
			payload.UploadVerified.PointId,
			payload.UploadVerified.StoredSizeBytes,
			payload.UploadVerified.StoredSha256,
		)
	case *agentpb.BackupCheckpointRequest_SourceCleanupCompleted:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_SOURCE_CLEANUP_COMPLETED &&
			payload.SourceCleanupCompleted != nil && validPoint(payload.SourceCleanupCompleted.PointId)
	case *agentpb.BackupCheckpointRequest_RestoreArtifactValidated:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_RESTORE_ARTIFACT_VALIDATED &&
			payload.RestoreArtifactValidated != nil && validPoint(payload.RestoreArtifactValidated.PointId) &&
			validDigest(payload.RestoreArtifactValidated.StoredSha256) &&
			validDigest(payload.RestoreArtifactValidated.DecodedSha256)
	case *agentpb.BackupCheckpointRequest_VolumeTreeStaged:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_STAGED &&
			payload.VolumeTreeStaged != nil && validPoint(payload.VolumeTreeStaged.PointId) &&
			validDigest(payload.VolumeTreeStaged.StagedTreeManifestSha256)
	case *agentpb.BackupCheckpointRequest_VolumeTreeExchanged:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_EXCHANGED &&
			payload.VolumeTreeExchanged != nil && validPoint(payload.VolumeTreeExchanged.PointId) &&
			validDigest(payload.VolumeTreeExchanged.LiveTreeManifestSha256)
	case *agentpb.BackupCheckpointRequest_VolumeReplacedTreeCleaned:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_REPLACED_TREE_CLEANED &&
			payload.VolumeReplacedTreeCleaned != nil && validPoint(payload.VolumeReplacedTreeCleaned.PointId)
	case *agentpb.BackupCheckpointRequest_ConfigGenerationStaged:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_STAGED &&
			payload.ConfigGenerationStaged != nil && validPoint(payload.ConfigGenerationStaged.PointId) &&
			ids.Validate(ids.KindConfig, payload.ConfigGenerationStaged.RestoreGenerationId) == nil &&
			validDigest(payload.ConfigGenerationStaged.EntryGenerationManifestSha256)
	case *agentpb.BackupCheckpointRequest_ConfigGenerationActivated:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_ACTIVATED &&
			payload.ConfigGenerationActivated != nil && validPoint(payload.ConfigGenerationActivated.PointId) &&
			ids.Validate(ids.KindConfig, payload.ConfigGenerationActivated.RestoreGenerationId) == nil &&
			payload.ConfigGenerationActivated.RenderGeneration > 0
	case *agentpb.BackupCheckpointRequest_PostgresRestoreVerified:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_POSTGRES_RESTORE_VERIFIED &&
			payload.PostgresRestoreVerified != nil && validPoint(payload.PostgresRestoreVerified.PointId)
	case *agentpb.BackupCheckpointRequest_RemoteObjectAbsent:
		valid = request.Kind == agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_REMOTE_OBJECT_ABSENT &&
			payload.RemoteObjectAbsent != nil && validPoint(payload.RemoteObjectAbsent.PointId)
	}
	if !valid {
		return errs.New(errs.KindValidationFailed, "Backup checkpoint kind or control payload is invalid")
	}
	return nil
}
