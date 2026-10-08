package executionplan

import (
	"bytes"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidateBackupTaskResume(
	resume *agentpb.BackupTaskResume,
	authority *agentpb.BackupTaskAuthority,
) (*agentpb.BackupTaskResume, error) {
	if err := validateBackupTaskAuthority(authority); err != nil {
		return nil, err
	}
	if resume == nil || RejectUnknown(resume) != nil || resume.AssignmentId != authority.AssignmentId ||
		resume.AssignmentGeneration != authority.AssignmentGeneration || len(resume.Steps) != len(authority.Steps) {
		return nil, errs.New(errs.KindValidationFailed, "backup resume assignment binding is invalid")
	}
	owned := proto.Clone(resume).(*agentpb.BackupTaskResume)
	for index, step := range owned.Steps {
		sealed := authority.Steps[index]
		if step == nil || step.StepId != sealed.StepId || step.ExecutionId != sealed.ExecutionId {
			return nil, errs.New(errs.KindValidationFailed, "backup resume step order or identity is invalid")
		}
		valid := false
		switch state := step.Operation.(type) {
		case *agentpb.BackupStepResume_Prune:
			valid = state != nil && validBackupPruneResume(state.Prune, sealed)
		case *agentpb.BackupStepResume_Capture:
			valid = state != nil && validBackupCaptureResume(state.Capture, sealed)
		case *agentpb.BackupStepResume_Restore:
			valid = state != nil && validBackupRestoreResume(state.Restore, sealed)
		}
		if !valid {
			return nil, errs.New(errs.KindValidationFailed, "backup resume progress is invalid or unsupported")
		}
	}
	return owned, nil
}

func validBackupPruneResume(state *agentpb.BackupPruneResume, step *agentpb.BackupStepAuthority) bool {
	if state == nil || step.GetPrune() == nil || len(step.GetPrune().Objects) != 1 {
		return false
	}
	object := step.GetPrune().Objects[0]
	if state.CheckpointSequence == 0 {
		return state.PrecedingCheckpoint == nil && state.Checkpoint == nil && state.NextObjectOrdinal == object.Ordinal
	}
	deleted := state.GetObjectDeleted()
	return state.CheckpointSequence == 1 && backupCheckpointFence(state.PrecedingCheckpoint, step.StepDigest) &&
		deleted != nil && deleted.Ordinal == object.Ordinal && deleted.PointId == object.PointId &&
		proto.Equal(deleted.Object, object.Object) && state.NextObjectOrdinal == object.Ordinal+1
}

func validBackupCaptureResume(state *agentpb.BackupCaptureResume, step *agentpb.BackupStepAuthority) bool {
	if state == nil || step.GetCapture() == nil || state.Cursor == nil || state.Cursor.ObjectAttempt != 1 {
		return false
	}
	prepared := state.PreparedArtifact
	if step.GetCapture().GetPostgres() == nil {
		if state.DumpStart != nil {
			return false
		}
	} else if state.CheckpointSequence < 2 {
		if state.DumpStart != nil {
			return false
		}
	} else if !postgresDumpMatches(state.DumpStart, step) {
		return false
	}
	if step.GetCapture().GetMysql() == nil {
		if state.MysqlDumpStart != nil {
			return false
		}
	} else if state.CheckpointSequence < 2 {
		if state.MysqlDumpStart != nil {
			return false
		}
	} else if !mysqlDumpMatches(state.MysqlDumpStart, step) {
		return false
	}
	if state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING {
		if prepared != nil {
			return false
		}
	} else if !validBackupArtifactPrepared(prepared) || prepared.PointId != step.GetCapture().PointId ||
		!backupResumePreparedSourceMatches(prepared, step.GetCapture()) {
		return false
	}
	if state.CheckpointSequence == 0 {
		return state.PrecedingCheckpoint == nil && state.Checkpoint == nil &&
			state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING &&
			state.Cursor.ConfigRecordSequence == 0 &&
			state.Cursor.ConfigValueOrdinal == 0 &&
			state.Cursor.ServiceCursor == 0 &&
			len(state.Cursor.CumulativeChainSha256) == 0
	}
	if !backupCheckpointFence(state.PrecedingCheckpoint, step.StepDigest) || state.Cursor.ServiceCursor != 0 {
		return false
	}
	if step.GetCapture().GetConfig() == nil {
		if state.Cursor.ConfigRecordSequence != 0 || state.Cursor.ConfigValueOrdinal != 0 ||
			len(state.Cursor.CumulativeChainSha256) != 0 {
			return false
		}
	} else if !validBackupConfigResumeCursor(state.Cursor.ConfigRecordSequence, state.Cursor.ConfigValueOrdinal, state.Cursor.CumulativeChainSha256, step.GetCapture().GetConfig().Content) {
		return false
	}
	pointID := step.GetCapture().PointId
	switch checkpoint := state.Checkpoint.(type) {
	case *agentpb.BackupCaptureResume_PostgresContainerObserved:
		return checkpoint != nil && postgresObservationMatches(checkpoint.PostgresContainerObserved, step) &&
			state.CheckpointSequence == 1 && state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING
	case *agentpb.BackupCaptureResume_PostgresDumpStart:
		return checkpoint != nil && postgresDumpMatches(checkpoint.PostgresDumpStart, step) &&
			proto.Equal(checkpoint.PostgresDumpStart, state.DumpStart) &&
			state.CheckpointSequence == 2 && state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING
	case *agentpb.BackupCaptureResume_MysqlContainerObserved:
		return checkpoint != nil && mysqlObservationMatches(checkpoint.MysqlContainerObserved, step) &&
			state.CheckpointSequence == 1 && state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING
	case *agentpb.BackupCaptureResume_MysqlDumpStartCheckpoint:
		return checkpoint != nil && mysqlDumpMatches(checkpoint.MysqlDumpStartCheckpoint, step) &&
			proto.Equal(checkpoint.MysqlDumpStartCheckpoint, state.MysqlDumpStart) &&
			state.CheckpointSequence == 2 && state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING
	case *agentpb.BackupCaptureResume_ConfigProgress:
		return checkpoint != nil && step.GetCapture().GetConfig() != nil && state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING &&
			validBackupConfigResumeProgress(checkpoint.ConfigProgress, step.GetCapture().GetConfig().Content, false) &&
			state.Cursor.ConfigRecordSequence > 0 && state.Cursor.ConfigValueOrdinal == checkpoint.ConfigProgress.NextValueOrdinal &&
			bytes.Equal(state.Cursor.CumulativeChainSha256, checkpoint.ConfigProgress.ValueChainSha256)
	case *agentpb.BackupCaptureResume_ArtifactPrepared:
		return checkpoint != nil && validBackupArtifactPrepared(checkpoint.ArtifactPrepared) && checkpoint.ArtifactPrepared.PointId == pointID &&
			proto.Equal(checkpoint.ArtifactPrepared, prepared) &&
			backupResumePreparedSourceMatches(checkpoint.ArtifactPrepared, step.GetCapture()) &&
			state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED
	case *agentpb.BackupCaptureResume_UploadCompleted:
		return checkpoint != nil && validBackupUploadCompleted(checkpoint.UploadCompleted) && checkpoint.UploadCompleted.PointId == pointID &&
			proto.Equal(checkpoint.UploadCompleted.Evidence, prepared.GetEvidence()) &&
			proto.Equal(checkpoint.UploadCompleted.Target, step.GetCapture().Target) && state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_HEAD_VERIFICATION
	case *agentpb.BackupCaptureResume_UploadVerified:
		return checkpoint != nil && checkpoint.UploadVerified != nil && validBackupCheckpointEvidence(checkpoint.UploadVerified.Evidence) &&
			proto.Equal(checkpoint.UploadVerified.Evidence, prepared.GetEvidence()) &&
			validBackupCheckpointObject(checkpoint.UploadVerified.Object, pointID) && validBackupCheckpointMetadata(checkpoint.UploadVerified.MetadataCount, checkpoint.UploadVerified.MetadataSha256) && checkpoint.UploadVerified.PointId == pointID &&
			backupResumeObjectMatchesTarget(checkpoint.UploadVerified.Object, step.GetCapture().Target) &&
			state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT
	case *agentpb.BackupCaptureResume_SourceCleanupCompleted:
		return checkpoint != nil && checkpoint.SourceCleanupCompleted != nil && checkpoint.SourceCleanupCompleted.PointId == pointID &&
			proto.Equal(checkpoint.SourceCleanupCompleted.Evidence, prepared.GetEvidence()) &&
			validBackupCheckpointEvidence(checkpoint.SourceCleanupCompleted.Evidence) && state.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY
	default:
		return false
	}
}
