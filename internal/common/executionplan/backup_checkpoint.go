package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const backupCheckpointDigestDomain = "groundplane.backup.checkpoint.schema-one.v1\x00"

// ValidateBackupCheckpointRequest owns a validated, unknown-field-free copy.
// Durable authority matching and atomic predecessor acceptance remain with
// the Controller; a valid request alone never authorizes an external mutation.
func ValidateBackupCheckpointRequest(
	request *agentpb.BackupCheckpointRequest,
	expectedSequence uint64,
) (*agentpb.BackupCheckpointRequest, error) {
	if request == nil || expectedSequence == 0 {
		return nil, invalidBackupCheckpoint("request or sequence is invalid")
	}
	owned := proto.Clone(request).(*agentpb.BackupCheckpointRequest)
	if err := validateBackupCheckpointRequest(owned); err != nil {
		return nil, err
	}
	if owned.CheckpointSequence != expectedSequence {
		return nil, invalidBackupCheckpoint("sequence does not match")
	}
	return owned, nil
}

// ComputeBackupCheckpointPayloadDigest hashes the complete deterministic
// schema-one request after rejecting unknown fields. The domain separates it
// from the retired grammar; identity, execution authority, predecessor fence,
// sequence and the typed checkpoint all participate in the digest.
func ComputeBackupCheckpointPayloadDigest(request *agentpb.BackupCheckpointRequest) ([]byte, error) {
	if err := validateBackupCheckpointRequest(request); err != nil {
		return nil, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	if len(encoded) > MaximumPlanBytes {
		return nil, invalidBackupCheckpoint("message exceeds the machine-message limit")
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(backupCheckpointDigestDomain))
	_, _ = hash.Write(encoded)
	return hash.Sum(nil), nil
}

func validateBackupCheckpointRequest(request *agentpb.BackupCheckpointRequest) error {
	if request == nil {
		return invalidBackupCheckpoint("request is required")
	}
	if err := RejectUnknown(request); err != nil {
		return err
	}
	if ids.Validate(ids.KindTask, request.TaskId) != nil ||
		ids.Validate(ids.KindAssignment, request.AssignmentId) != nil ||
		ids.Validate(ids.KindStep, request.StepId) != nil ||
		!validRawULID(request.ExecutionId) || request.CheckpointSequence == 0 ||
		!backupCheckpointDigest(request.AuthorityDigest) {
		return invalidBackupCheckpoint("delivery identity is invalid")
	}
	if request.CheckpointSequence == 1 {
		if request.PrecedingCheckpoint != nil {
			return invalidBackupCheckpoint("first checkpoint cannot carry a predecessor")
		}
	} else if !backupCheckpointFence(request.PrecedingCheckpoint, request.AuthorityDigest) {
		return invalidBackupCheckpoint("predecessor fence is invalid")
	}
	return validateBackupCheckpointPayload(request)
}

// ValidateBackupCheckpointAck verifies the full delivery tuple and a committed
// durable fence from the same authority. Its positive revision proves that an
// identity-only echo cannot release the Agent's write-before-mutation boundary.
func ValidateBackupCheckpointAck(
	ack *agentpb.BackupCheckpointAck,
	request *agentpb.BackupCheckpointRequest,
) (*agentpb.BackupCheckpointAck, error) {
	if ack == nil {
		return nil, invalidBackupCheckpoint("acknowledgement is required")
	}
	if err := validateBackupCheckpointRequest(request); err != nil {
		return nil, err
	}
	owned := proto.Clone(ack).(*agentpb.BackupCheckpointAck)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	if owned.TaskId != request.TaskId || owned.AssignmentId != request.AssignmentId ||
		owned.StepId != request.StepId || owned.ExecutionId != request.ExecutionId ||
		owned.CheckpointSequence != request.CheckpointSequence ||
		!backupCheckpointFence(owned.Committed, request.AuthorityDigest) {
		return nil, invalidBackupCheckpoint("acknowledgement identity or committed fence is invalid")
	}
	if request.PrecedingCheckpoint != nil &&
		owned.Committed.DedupeKeyModRevision <= request.PrecedingCheckpoint.DedupeKeyModRevision {
		return nil, invalidBackupCheckpoint("committed fence does not advance its predecessor")
	}
	return owned, nil
}

func backupCheckpointFence(fence *agentpb.CheckpointFence, authority []byte) bool {
	return fence != nil && fence.DedupeKeyModRevision > 0 &&
		backupCheckpointDigest(fence.AuthorityDigest) && bytes.Equal(fence.AuthorityDigest, authority)
}

func backupCheckpointDigest(value []byte) bool { return len(value) == sha256.Size }

func backupCheckpointPoint(value string) bool {
	return ids.Validate(ids.KindRecoveryPoint, value) == nil
}

func invalidBackupCheckpoint(message string) error {
	return errs.New(errs.KindValidationFailed, "backup checkpoint "+message)
}

func validateBackupCheckpointPayload(request *agentpb.BackupCheckpointRequest) error {
	valid := false
	switch checkpoint := request.Checkpoint.(type) {
	case *agentpb.BackupCheckpointRequest_ArtifactPrepared:
		valid = checkpoint != nil && validBackupArtifactPrepared(checkpoint.ArtifactPrepared)
	case *agentpb.BackupCheckpointRequest_UploadCompleted:
		valid = checkpoint != nil && validBackupUploadCompleted(checkpoint.UploadCompleted)
	case *agentpb.BackupCheckpointRequest_UploadVerified:
		if checkpoint == nil {
			break
		}
		value := checkpoint.UploadVerified
		valid = value != nil && backupCheckpointPoint(value.PointId) &&
			validBackupCheckpointEvidence(value.Evidence) &&
			validBackupCheckpointObject(value.Object, value.PointId) &&
			validBackupCheckpointMetadata(value.MetadataCount, value.MetadataSha256)
	case *agentpb.BackupCheckpointRequest_SourceCleanupCompleted:
		if checkpoint == nil {
			break
		}
		value := checkpoint.SourceCleanupCompleted
		valid = value != nil && backupCheckpointPoint(value.PointId) && validBackupCheckpointEvidence(value.Evidence)
	case *agentpb.BackupCheckpointRequest_RestoreArtifactValidated:
		valid = checkpoint != nil && validBackupRestoreArtifact(checkpoint.RestoreArtifactValidated)
	case *agentpb.BackupCheckpointRequest_PostgresContainerObserved:
		valid = checkpoint != nil && validBackupPostgresObservation(checkpoint.PostgresContainerObserved)
	case *agentpb.BackupCheckpointRequest_PostgresDumpStart:
		valid = checkpoint != nil && validBackupPostgresDump(checkpoint.PostgresDumpStart)
	case *agentpb.BackupCheckpointRequest_PostgresRestoreApplyStart:
		valid = checkpoint != nil && validBackupPostgresApply(checkpoint.PostgresRestoreApplyStart)
	case *agentpb.BackupCheckpointRequest_PostgresRestoreVerified:
		if checkpoint == nil {
			break
		}
		value := checkpoint.PostgresRestoreVerified
		valid = value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointHexID(value.ContainerId) &&
			validBackupCheckpointEvidence(value.Evidence) && backupCheckpointDigest(value.VerificationSha256)
	case *agentpb.BackupCheckpointRequest_PostgresServiceProgress:
		valid = checkpoint != nil && validBackupPostgresService(checkpoint.PostgresServiceProgress)
	case *agentpb.BackupCheckpointRequest_MysqlContainerObserved:
		valid = checkpoint != nil && validBackupMySQLObservation(checkpoint.MysqlContainerObserved)
	case *agentpb.BackupCheckpointRequest_MysqlDumpStart:
		valid = checkpoint != nil && validBackupMySQLDump(checkpoint.MysqlDumpStart)
	case *agentpb.BackupCheckpointRequest_MysqlRestoreApplyStart:
		valid = checkpoint != nil && validBackupMySQLApply(checkpoint.MysqlRestoreApplyStart)
	case *agentpb.BackupCheckpointRequest_MysqlRestoreVerified:
		if checkpoint == nil {
			break
		}
		value := checkpoint.MysqlRestoreVerified
		valid = value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointHexID(value.ContainerId) &&
			validBackupCheckpointEvidence(value.Evidence) && backupCheckpointDigest(value.VerificationSha256)
	case *agentpb.BackupCheckpointRequest_MysqlServiceProgress:
		valid = checkpoint != nil && validBackupMySQLService(checkpoint.MysqlServiceProgress)
	case *agentpb.BackupCheckpointRequest_Config:
		valid = checkpoint != nil && validBackupConfigCheckpoint(checkpoint.Config)
	case *agentpb.BackupCheckpointRequest_Volume:
		valid = checkpoint != nil && validBackupVolumeCheckpoint(checkpoint.Volume)
	case *agentpb.BackupCheckpointRequest_PruneObjectDeleted:
		if checkpoint == nil {
			break
		}
		value := checkpoint.PruneObjectDeleted
		valid = value != nil && value.Ordinal > 0 && backupCheckpointPoint(value.PointId) &&
			validBackupCheckpointObject(value.Object, value.PointId)
	}
	if !valid {
		return invalidBackupCheckpoint("typed payload is invalid")
	}
	return nil
}
