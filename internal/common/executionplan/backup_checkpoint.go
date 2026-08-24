package executionplan

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"

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
	digest, err := ComputeBackupCheckpointPayloadDigest(owned)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(digest, owned.ControlPayloadSha256) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint control payload digest does not match")
	}
	return owned, nil
}

// ComputeBackupCheckpointPayloadDigest implements the ADR 0024 version-one
// protobuf-independent byte grammar. Assignment identity and the outer digest
// are deliberately outside this Controller-protected control-payload digest.
func ComputeBackupCheckpointPayloadDigest(request *agentpb.BackupCheckpointRequest) ([]byte, error) {
	if request == nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup checkpoint request is required")
	}
	if err := RejectUnknown(request); err != nil {
		return nil, err
	}
	if err := validateBackupCheckpointPayload(request); err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	encoded.WriteString("groundplane.backup.checkpoint.v1")
	encoded.WriteByte(0)
	writeCheckpointUint32(&encoded, uint32(request.Kind))
	writePoint := func(pointID string) { writeCheckpointString(&encoded, pointID) }
	switch payload := request.Payload.(type) {
	case *agentpb.BackupCheckpointRequest_ArtifactPrepared:
		writePoint(payload.ArtifactPrepared.PointId)
		writeCheckpointUint64(&encoded, payload.ArtifactPrepared.StoredSizeBytes)
		encoded.Write(payload.ArtifactPrepared.StoredSha256)
	case *agentpb.BackupCheckpointRequest_UploadVerified:
		writePoint(payload.UploadVerified.PointId)
		writeCheckpointUint64(&encoded, payload.UploadVerified.StoredSizeBytes)
		encoded.Write(payload.UploadVerified.StoredSha256)
	case *agentpb.BackupCheckpointRequest_SourceCleanupCompleted:
		writePoint(payload.SourceCleanupCompleted.PointId)
	case *agentpb.BackupCheckpointRequest_RestoreArtifactValidated:
		writePoint(payload.RestoreArtifactValidated.PointId)
		encoded.Write(payload.RestoreArtifactValidated.StoredSha256)
		encoded.Write(payload.RestoreArtifactValidated.DecodedSha256)
	case *agentpb.BackupCheckpointRequest_VolumeTreeStaged:
		writePoint(payload.VolumeTreeStaged.PointId)
		encoded.Write(payload.VolumeTreeStaged.StagedTreeManifestSha256)
	case *agentpb.BackupCheckpointRequest_VolumeTreeExchanged:
		writePoint(payload.VolumeTreeExchanged.PointId)
		encoded.Write(payload.VolumeTreeExchanged.LiveTreeManifestSha256)
	case *agentpb.BackupCheckpointRequest_VolumeReplacedTreeCleaned:
		writePoint(payload.VolumeReplacedTreeCleaned.PointId)
	case *agentpb.BackupCheckpointRequest_ConfigGenerationStaged:
		writePoint(payload.ConfigGenerationStaged.PointId)
		writeCheckpointString(&encoded, payload.ConfigGenerationStaged.RestoreGenerationId)
		encoded.Write(payload.ConfigGenerationStaged.EntryGenerationManifestSha256)
	case *agentpb.BackupCheckpointRequest_ConfigGenerationActivated:
		writePoint(payload.ConfigGenerationActivated.PointId)
		writeCheckpointString(&encoded, payload.ConfigGenerationActivated.RestoreGenerationId)
		writeCheckpointUint64(&encoded, payload.ConfigGenerationActivated.RenderGeneration)
	case *agentpb.BackupCheckpointRequest_PostgresRestoreVerified:
		writePoint(payload.PostgresRestoreVerified.PointId)
	case *agentpb.BackupCheckpointRequest_RemoteObjectAbsent:
		writePoint(payload.RemoteObjectAbsent.PointId)
	}
	digest := sha256.Sum256(encoded.Bytes())
	return append([]byte(nil), digest[:]...), nil
}

func writeCheckpointString(target *bytes.Buffer, value string) {
	writeCheckpointUint32(target, uint32(len(value)))
	target.WriteString(value)
}

func writeCheckpointUint32(target *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	target.Write(encoded[:])
}

func writeCheckpointUint64(target *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	target.Write(encoded[:])
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
