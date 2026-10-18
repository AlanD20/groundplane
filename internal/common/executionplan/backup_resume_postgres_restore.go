package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validPostgresRestoreResume(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority) bool {
	if state == nil || step.GetRestore().GetPostgres() == nil || state.Cursor == nil ||
		state.Cursor.ObjectAttempt != 1 || state.Cursor.ConfigRecordSequence != 0 || state.Cursor.ConfigValueOrdinal != 0 ||
		state.Cursor.VolumeCursor != 0 || len(state.Cursor.CumulativeChainSha256) != 0 ||
		uint64(state.Cursor.ServiceCursor) > uint64(len(step.ConsumerServiceIds)) {
		return false
	}
	if state.Phase < agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION {
		if state.ApplyStart != nil {
			return false
		}
	} else if !validBackupPostgresApply(state.ApplyStart) || state.ApplyStart.PointId != step.GetRestore().PointId ||
		state.ApplyStart.SourceSizeBytes != step.GetRestore().ExpectedEvidence.SourceSizeBytes ||
		!bytes.Equal(state.ApplyStart.SourceSha256, step.GetRestore().ExpectedEvidence.SourceSha256) {
		return false
	}
	if state.CheckpointSequence == 0 {
		return state.PrecedingCheckpoint == nil && state.Checkpoint == nil && state.Cursor.ServiceCursor == 0 &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION
	}
	if !backupCheckpointFence(state.PrecedingCheckpoint, step.StepDigest) {
		return false
	}
	restore := step.GetRestore()
	switch {
	case state.GetArtifactValidated() != nil:
		return backupResumeRestoreArtifactMatches(state.GetArtifactValidated(), restore) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
	case state.GetPostgresContainerObserved() != nil:
		return postgresObservationMatches(state.GetPostgresContainerObserved(), step) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
	case state.GetPostgresRestoreApplyStart() != nil:
		value := state.GetPostgresRestoreApplyStart()
		return validBackupPostgresApply(value) && value.PointId == restore.PointId &&
			proto.Equal(value, state.ApplyStart) &&
			value.SourceSizeBytes == restore.ExpectedEvidence.SourceSizeBytes &&
			bytes.Equal(value.SourceSha256, restore.ExpectedEvidence.SourceSha256) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION
	case state.GetPostgresRestoreVerified() != nil:
		value := state.GetPostgresRestoreVerified()
		return value.PointId == restore.PointId && backupCheckpointHexID(value.ContainerId) &&
			backupCheckpointDigest(value.VerificationSha256) && proto.Equal(value.Evidence, restore.ExpectedEvidence) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
	case state.GetPostgresServiceProgress() != nil:
		value := state.GetPostgresServiceProgress()
		return validBackupPostgresService(value) && int(value.ServiceCursor) < len(step.ConsumerServiceIds) &&
			value.ServiceId == step.ConsumerServiceIds[value.ServiceCursor] && state.Cursor.ServiceCursor == value.ServiceCursor &&
			(state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION ||
				state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY)
	case state.GetSourceCleanupCompleted() != nil:
		value := state.GetSourceCleanupCompleted()
		return value.PointId == restore.PointId && proto.Equal(value.Evidence, restore.ExpectedEvidence) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TERMINAL
	default:
		return false
	}
}
