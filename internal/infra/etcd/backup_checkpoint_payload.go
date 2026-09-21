package etcd

import (
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateBackupCheckpointInput(input BackupCheckpointInput) error {
	if recordcodec.ValidateID(ids.KindTask, input.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindAssignment, input.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindStep, input.StepID) != nil || input.Sequence == 0 {
		return errs.New(errs.KindValidationFailed, "backup checkpoint identity is invalid")
	}
	if input.AgentID == "" {
		if input.AgentGeneration != 0 {
			return errs.New(
				errs.KindValidationFailed,
				"controller checkpoint agent identity is invalid",
			)
		}
	} else if recordcodec.ValidateID(ids.KindAgent, input.AgentID) != nil || input.AgentGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "agent checkpoint identity is invalid")
	}
	return nil
}

func backupCheckpointDigest(payload BackupCheckpointPayload) (string, error) {
	request := &agentpb.BackupCheckpointRequest{}
	remaining := payload
	remaining.Kind = 0
	remaining.PointID = ""
	decodeDigest := func(value string) ([]byte, error) {
		if !recordcodec.ValidSHA256(value) {
			return nil, errs.New(errs.KindValidationFailed, "backup checkpoint digest field is invalid")
		}
		decoded, err := hex.DecodeString(value)
		if err != nil {
			return nil, errs.New(errs.KindValidationFailed, "backup checkpoint digest field is invalid")
		}
		return decoded, nil
	}
	switch payload.Kind {
	case BackupCheckpointArtifactPrepared:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED
		request.Payload = &agentpb.BackupCheckpointRequest_ArtifactPrepared{
			ArtifactPrepared: &agentpb.BackupArtifactPreparedCheckpoint{
				PointId: payload.PointID, StoredSizeBytes: payload.StoredSizeBytes, StoredSha256: storedSHA256,
			},
		}
		remaining.StoredSizeBytes = 0
		remaining.StoredSHA256 = ""
	case BackupCheckpointUploadVerified:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_VERIFIED
		request.Payload = &agentpb.BackupCheckpointRequest_UploadVerified{
			UploadVerified: &agentpb.BackupUploadVerifiedCheckpoint{
				PointId: payload.PointID, StoredSizeBytes: payload.StoredSizeBytes, StoredSha256: storedSHA256,
			},
		}
		remaining.StoredSizeBytes = 0
		remaining.StoredSHA256 = ""
	case BackupCheckpointSourceCleanupCompleted:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_SOURCE_CLEANUP_COMPLETED
		request.Payload = &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
			SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompletedCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointRestoreArtifactValidated:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		decodedSHA256, err := decodeDigest(payload.DecodedSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_RESTORE_ARTIFACT_VALIDATED
		request.Payload = &agentpb.BackupCheckpointRequest_RestoreArtifactValidated{
			RestoreArtifactValidated: &agentpb.BackupRestoreArtifactValidatedCheckpoint{
				PointId: payload.PointID, StoredSha256: storedSHA256, DecodedSha256: decodedSHA256,
			},
		}
		remaining.StoredSHA256 = ""
		remaining.DecodedSHA256 = ""
	case BackupCheckpointVolumeTreeStaged:
		manifestSHA256, err := decodeDigest(payload.StagedTreeManifestSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_STAGED
		request.Payload = &agentpb.BackupCheckpointRequest_VolumeTreeStaged{
			VolumeTreeStaged: &agentpb.BackupVolumeTreeStagedCheckpoint{
				PointId: payload.PointID, StagedTreeManifestSha256: manifestSHA256,
			},
		}
		remaining.StagedTreeManifestSHA256 = ""
	case BackupCheckpointVolumeTreeExchanged:
		manifestSHA256, err := decodeDigest(payload.LiveTreeManifestSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_EXCHANGED
		request.Payload = &agentpb.BackupCheckpointRequest_VolumeTreeExchanged{
			VolumeTreeExchanged: &agentpb.BackupVolumeTreeExchangedCheckpoint{
				PointId: payload.PointID, LiveTreeManifestSha256: manifestSHA256,
			},
		}
		remaining.LiveTreeManifestSHA256 = ""
	case BackupCheckpointVolumeReplacedTreeCleaned:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_REPLACED_TREE_CLEANED
		request.Payload = &agentpb.BackupCheckpointRequest_VolumeReplacedTreeCleaned{
			VolumeReplacedTreeCleaned: &agentpb.BackupVolumeReplacedTreeCleanedCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointConfigGenerationStaged:
		manifestSHA256, err := decodeDigest(payload.EntryGenerationManifestSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_STAGED
		request.Payload = &agentpb.BackupCheckpointRequest_ConfigGenerationStaged{
			ConfigGenerationStaged: &agentpb.BackupConfigGenerationStagedCheckpoint{
				PointId: payload.PointID, RestoreGenerationId: payload.RestoreGenerationID,
				EntryGenerationManifestSha256: manifestSHA256,
			},
		}
		remaining.RestoreGenerationID = ""
		remaining.EntryGenerationManifestSHA256 = ""
	case BackupCheckpointConfigGenerationActivated:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_ACTIVATED
		request.Payload = &agentpb.BackupCheckpointRequest_ConfigGenerationActivated{
			ConfigGenerationActivated: &agentpb.BackupConfigGenerationActivatedCheckpoint{
				PointId: payload.PointID, RestoreGenerationId: payload.RestoreGenerationID,
				RenderGeneration: payload.RenderGeneration,
			},
		}
		remaining.RestoreGenerationID = ""
		remaining.RenderGeneration = 0
	case BackupCheckpointPostgresRestoreVerified:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_POSTGRES_RESTORE_VERIFIED
		request.Payload = &agentpb.BackupCheckpointRequest_PostgresRestoreVerified{
			PostgresRestoreVerified: &agentpb.BackupPostgresRestoreVerifiedCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointRemoteObjectAbsent:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_REMOTE_OBJECT_ABSENT
		request.Payload = &agentpb.BackupCheckpointRequest_RemoteObjectAbsent{
			RemoteObjectAbsent: &agentpb.BackupRemoteObjectAbsentCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointUploadCompleted:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_COMPLETED
		request.Payload = &agentpb.BackupCheckpointRequest_UploadCompleted{
			UploadCompleted: &agentpb.BackupUploadCompletedCheckpoint{
				PointId: payload.PointID, StoredSizeBytes: payload.StoredSizeBytes, StoredSha256: storedSHA256,
			},
		}
		remaining.StoredSizeBytes = 0
		remaining.StoredSHA256 = ""
	default:
		return "", errs.New(errs.KindValidationFailed, "backup checkpoint kind is invalid")
	}
	if remaining != (BackupCheckpointPayload{}) {
		return "", errs.New(errs.KindValidationFailed, "backup checkpoint payload has fields outside its kind")
	}
	digest, err := executionplan.ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest), nil
}
