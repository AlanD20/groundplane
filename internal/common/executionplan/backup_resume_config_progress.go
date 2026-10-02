package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validBackupRestoreResume(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority) bool {
	if state == nil || step == nil || step.GetRestore() == nil {
		return false
	}
	if step.GetRestore().GetPostgres() != nil {
		return validPostgresRestoreResume(state, step)
	}
	if state.ApplyStart != nil {
		return false
	}
	if step.GetRestore().GetVolume() != nil {
		return validBackupVolumeRestoreResume(state, step)
	}
	if state == nil || step.GetRestore() == nil || state.Cursor == nil || state.Cursor.ObjectAttempt != 1 ||
		state.Cursor.VolumeCursor != 0 || state.Cursor.ServiceCursor != 0 {
		return false
	}
	if state.CheckpointSequence == 0 {
		return state.PrecedingCheckpoint == nil && state.Checkpoint == nil &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION &&
			state.Cursor.ConfigRecordSequence == 0 && state.Cursor.ConfigValueOrdinal == 0 && len(state.Cursor.CumulativeChainSha256) == 0
	}
	if !backupCheckpointFence(state.PrecedingCheckpoint, step.StepDigest) {
		return false
	}
	config := step.GetRestore().GetConfig()
	if config == nil {
		if state.Cursor.ConfigRecordSequence != 0 || state.Cursor.ConfigValueOrdinal != 0 ||
			len(state.Cursor.CumulativeChainSha256) != 0 {
			return false
		}
	} else if !validBackupConfigResumeCursor(state.Cursor.ConfigRecordSequence, state.Cursor.ConfigValueOrdinal, state.Cursor.CumulativeChainSha256, config.GetExpectedArchive().GetContent()) {
		return false
	}
	switch checkpoint := state.Checkpoint.(type) {
	case *agentpb.BackupRestoreResume_ArtifactValidated:
		return checkpoint != nil && backupResumeRestoreArtifactMatches(checkpoint.ArtifactValidated, step.GetRestore()) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
	case *agentpb.BackupRestoreResume_ConfigProgress:
		if checkpoint == nil || config == nil || !validBackupConfigResumeProgress(checkpoint.ConfigProgress, config.GetExpectedArchive().GetContent(), true) {
			return false
		}
		if transfer := checkpoint.ConfigProgress.GetTransfer(); transfer != nil &&
			(transfer.RestoreGenerationId != config.RestoreGenerationId || transfer.RenderGeneration != config.RenderGeneration) {
			return false
		}
		if materialized := checkpoint.ConfigProgress.GetMaterialization(); materialized != nil &&
			materialized.RestoreGenerationId != config.RestoreGenerationId {
			return false
		}
		phase := agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
		if checkpoint.ConfigProgress.GetTransfer() != nil {
			phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_PUBLICATION
		}
		if checkpoint.ConfigProgress.GetMaterialization() != nil {
			phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_CLEANUP
		}
		if cleanup := checkpoint.ConfigProgress.SourceCleanupCompleted; cleanup != nil {
			if cleanup.PointId != step.GetRestore().PointId || !proto.Equal(cleanup.Evidence, step.GetRestore().ExpectedEvidence) {
				return false
			}
			phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		}
		return state.Phase == phase && state.Cursor.ConfigRecordSequence > 0 && state.Cursor.ConfigValueOrdinal == checkpoint.ConfigProgress.NextValueOrdinal &&
			bytes.Equal(state.Cursor.CumulativeChainSha256, checkpoint.ConfigProgress.ValueChainSha256)
	default:
		return false
	}
}

func validBackupConfigResumeCursor(
	records uint64,
	ordinal uint32,
	chain []byte,
	content *agentpb.BackupConfigContentAuthority,
) bool {
	if records == 0 && ordinal == 0 && len(chain) == 0 {
		return true
	}
	return records > 0 && ordinal > 0 && ordinal <= content.EntryCount+1 && backupCheckpointDigest(chain)
}

func validBackupConfigResumeProgress(
	progress *agentpb.BackupConfigProgress,
	content *agentpb.BackupConfigContentAuthority,
	restore bool,
) bool {
	if progress == nil || progress.MetadataAccepted == nil || !progress.GetMetadataAccepted() ||
		!backupCheckpointDigest(
			progress.MetadataTranscriptSha256,
		) || !backupCheckpointDigest(progress.ValueChainSha256) ||
		progress.NextValueOrdinal == 0 || progress.NextValueOrdinal > content.EntryCount+1 {
		return false
	}
	if cleanup := progress.SourceCleanupCompleted; cleanup != nil &&
		(!restore || progress.GetMaterialization() == nil || !backupCheckpointPoint(cleanup.PointId) || !validBackupCheckpointEvidence(cleanup.Evidence)) {
		return false
	}
	switch state := progress.Progress.(type) {
	case nil:
		return true
	case *agentpb.BackupConfigProgress_Transfer:
		return state != nil && validBackupConfigTransferCompleted(state.Transfer) && proto.Equal(state.Transfer.Content, content) &&
			((restore && state.Transfer.RestoreGenerationId != "" && state.Transfer.RenderGeneration > 0) ||
				(!restore && state.Transfer.RestoreGenerationId == "" && state.Transfer.RenderGeneration == 0)) &&
			progress.NextValueOrdinal == content.EntryCount+1 && bytes.Equal(progress.ValueChainSha256, state.Transfer.ValueChainSha256)
	case *agentpb.BackupConfigProgress_Materialization:
		return restore && state != nil && state.Materialization != nil &&
			validBackupConfigCheckpoint(&agentpb.BackupConfigCheckpoint{Checkpoint: &agentpb.BackupConfigCheckpoint_MaterializationVerified{MaterializationVerified: state.Materialization}}) &&
			state.Materialization.MaterializedEntryCount == content.EntryCount && progress.NextValueOrdinal == content.EntryCount+1
	default:
		return false
	}
}

func backupResumePreparedSourceMatches(
	prepared *agentpb.BackupArtifactPrepared,
	capture *agentpb.BackupCaptureAuthority,
) bool {
	switch {
	case capture.GetConfig() != nil:
		return prepared.GetConfig() != nil && proto.Equal(prepared.GetConfig().Content, capture.GetConfig().Content)
	case capture.GetVolume() != nil:
		return prepared.GetVolume() != nil
	case capture.GetPostgres() != nil:
		return prepared.GetPostgres() != nil
	default:
		return false
	}
}

func backupResumeObjectMatchesTarget(object *agentpb.BackupObjectIdentity, target *agentpb.BackupObjectTarget) bool {
	return object != nil && target != nil && object.Bucket == target.Bucket && object.ObjectKey == target.ObjectKey &&
		proto.Equal(object.Connector, target.Connector)
}

func backupResumeRestoreArtifactMatches(
	value *agentpb.BackupRestoreArtifactValidated,
	restore *agentpb.BackupRestoreAuthority,
) bool {
	if !validBackupRestoreArtifact(value) || value.PointId != restore.PointId ||
		!proto.Equal(value.Object, restore.SourceObject) ||
		!proto.Equal(value.Evidence, restore.ExpectedEvidence) {
		return false
	}
	switch {
	case restore.GetConfig() != nil:
		return value.GetConfig() != nil && proto.Equal(value.GetConfig(), restore.GetConfig().ExpectedArchive)
	case restore.GetVolume() != nil:
		return value.GetVolume() != nil && proto.Equal(value.GetVolume(), restore.GetVolume().Archive)
	case restore.GetPostgres() != nil:
		return value.GetPostgres() != nil
	default:
		return false
	}
}
