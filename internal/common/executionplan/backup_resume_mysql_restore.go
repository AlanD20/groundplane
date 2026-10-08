package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validMySQLRestoreResume(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority) bool {
	if state == nil || step.GetRestore().GetMysql() == nil || state.Cursor == nil || state.Cursor.ObjectAttempt != 1 ||
		state.Cursor.ConfigRecordSequence != 0 || state.Cursor.ConfigValueOrdinal != 0 || state.Cursor.VolumeCursor != 0 ||
		len(
			state.Cursor.CumulativeChainSha256,
		) != 0 || uint64(state.Cursor.ServiceCursor) > uint64(len(step.ConsumerServiceIds)) ||
		state.ApplyStart != nil {
		return false
	}
	if state.Phase < agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION {
		if state.MysqlApplyStart != nil {
			return false
		}
	} else if !validBackupMySQLApply(state.MysqlApplyStart) || state.MysqlApplyStart.PointId != step.GetRestore().PointId ||
		state.MysqlApplyStart.SourceSizeBytes != step.GetRestore().ExpectedEvidence.SourceSizeBytes ||
		!bytes.Equal(state.MysqlApplyStart.SourceSha256, step.GetRestore().ExpectedEvidence.SourceSha256) {
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
	case state.GetMysqlContainerObserved() != nil:
		return mysqlObservationMatches(state.GetMysqlContainerObserved(), step) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
	case state.GetMysqlRestoreApplyStart() != nil:
		value := state.GetMysqlRestoreApplyStart()
		return validBackupMySQLApply(value) && value.PointId == restore.PointId &&
			proto.Equal(value, state.MysqlApplyStart) &&
			value.SourceSizeBytes == restore.ExpectedEvidence.SourceSizeBytes &&
			bytes.Equal(value.SourceSha256, restore.ExpectedEvidence.SourceSha256) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION
	case state.GetMysqlRestoreVerified() != nil:
		value := state.GetMysqlRestoreVerified()
		return value.PointId == restore.PointId && backupCheckpointHexID(value.ContainerId) &&
			backupCheckpointDigest(value.VerificationSha256) && proto.Equal(value.Evidence, restore.ExpectedEvidence) &&
			state.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
	case state.GetMysqlServiceProgress() != nil:
		value := state.GetMysqlServiceProgress()
		return validBackupMySQLService(value) && int(value.ServiceCursor) < len(step.ConsumerServiceIds) &&
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

func (replay *BackupStepResumeReplay) mysqlRestoreCheckpoint(request *agentpb.BackupCheckpointRequest) error {
	step, state := replay.step, replay.restore
	restore := step.GetRestore()
	if restore.GetMysql() == nil || state.Checkpoint == nil || state.GetSourceCleanupCompleted() != nil {
		return backupResumeHistoryInvalid()
	}
	switch {
	case request.GetMysqlContainerObserved() != nil:
		value := request.GetMysqlContainerObserved()
		if state.GetArtifactValidated() == nil || replay.mysqlObserved != nil ||
			!mysqlObservationMatchesService(value, step, replay.authority.Services) {
			return backupResumeHistoryInvalid()
		}
		replay.mysqlObserved = value
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlContainerObserved{MysqlContainerObserved: value}
	case request.GetMysqlServiceProgress() != nil:
		if replay.mysqlObserved == nil || replay.mysqlApplyStart != nil && replay.mysqlVerified == nil {
			return backupResumeHistoryInvalid()
		}
		value := request.GetMysqlServiceProgress()
		if err := replay.databaseConsumerCheckpoint(value.ServiceCursor, value.ServiceId, value.Phase,
			value.ObservationSha256, replay.mysqlVerified != nil); err != nil {
			return err
		}
		state.Cursor.ServiceCursor = value.ServiceCursor
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlServiceProgress{MysqlServiceProgress: value}
	case request.GetMysqlRestoreApplyStart() != nil:
		value, observed := request.GetMysqlRestoreApplyStart(), replay.mysqlObserved
		if !validBackupMySQLApply(value) || observed == nil || replay.mysqlApplyStart != nil ||
			int(replay.databaseStopped) != len(step.ConsumerServiceIds) || value.PointId != restore.PointId ||
			value.ContainerId != observed.ContainerId ||
			!bytes.Equal(value.ImageReferenceSha256, observed.ImageReferenceSha256) ||
			!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) ||
			value.SourceSizeBytes != restore.ExpectedEvidence.SourceSizeBytes ||
			!bytes.Equal(value.SourceSha256, restore.ExpectedEvidence.SourceSha256) {
			return backupResumeHistoryInvalid()
		}
		replay.mysqlApplyStart = value
		state.MysqlApplyStart = proto.CloneOf(value)
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlRestoreApplyStart{MysqlRestoreApplyStart: value}
	case request.GetMysqlRestoreVerified() != nil:
		value := request.GetMysqlRestoreVerified()
		if replay.mysqlApplyStart == nil || replay.mysqlVerified != nil || value.PointId != restore.PointId ||
			value.ContainerId != replay.mysqlApplyStart.ContainerId || !proto.Equal(value.Evidence, restore.ExpectedEvidence) ||
			!backupCheckpointDigest(value.VerificationSha256) {
			return backupResumeHistoryInvalid()
		}
		replay.mysqlVerified = value
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		state.Checkpoint = &agentpb.BackupRestoreResume_MysqlRestoreVerified{MysqlRestoreVerified: value}
	case request.GetSourceCleanupCompleted() != nil:
		value := request.GetSourceCleanupCompleted()
		if replay.mysqlVerified == nil || int(replay.databaseRecovered) != len(step.ConsumerServiceIds) ||
			value.PointId != restore.PointId || !proto.Equal(value.Evidence, restore.ExpectedEvidence) {
			return backupResumeHistoryInvalid()
		}
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TERMINAL
		state.Checkpoint = &agentpb.BackupRestoreResume_SourceCleanupCompleted{SourceCleanupCompleted: value}
	default:
		return backupResumeHistoryInvalid()
	}
	return nil
}
