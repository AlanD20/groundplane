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

func (replay *BackupStepResumeReplay) postgresRestoreCheckpoint(request *agentpb.BackupCheckpointRequest) error {
	step, state := replay.step, replay.restore
	restore := step.GetRestore()
	if restore.GetPostgres() == nil || state.Checkpoint == nil || state.GetSourceCleanupCompleted() != nil {
		return backupResumeHistoryInvalid()
	}
	switch {
	case request.GetPostgresContainerObserved() != nil:
		value := request.GetPostgresContainerObserved()
		if state.GetArtifactValidated() == nil || replay.postgresObserved != nil ||
			!postgresObservationMatchesService(value, step, replay.authority.Services) {
			return backupResumeHistoryInvalid()
		}
		replay.postgresObserved = value
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresContainerObserved{PostgresContainerObserved: value}
	case request.GetPostgresServiceProgress() != nil:
		if replay.postgresObserved == nil || (replay.postgresApplyStart != nil && replay.postgresVerified == nil) {
			return backupResumeHistoryInvalid()
		}
		if err := replay.postgresConsumerCheckpoint(request.GetPostgresServiceProgress()); err != nil {
			return err
		}
		value := request.GetPostgresServiceProgress()
		state.Cursor.ServiceCursor = value.ServiceCursor
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresServiceProgress{PostgresServiceProgress: value}
	case request.GetPostgresRestoreApplyStart() != nil:
		value, observed := request.GetPostgresRestoreApplyStart(), replay.postgresObserved
		if !validBackupPostgresApply(value) || observed == nil || replay.postgresApplyStart != nil ||
			int(replay.postgresStopped) != len(step.ConsumerServiceIds) || value.PointId != restore.PointId ||
			value.ContainerId != observed.ContainerId || !bytes.Equal(value.RepositoryDigest, observed.RepositoryDigest) ||
			!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) ||
			value.SourceSizeBytes != restore.ExpectedEvidence.SourceSizeBytes ||
			!bytes.Equal(value.SourceSha256, restore.ExpectedEvidence.SourceSha256) {
			return backupResumeHistoryInvalid()
		}
		replay.postgresApplyStart = value
		state.ApplyStart = proto.CloneOf(value)
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresRestoreApplyStart{PostgresRestoreApplyStart: value}
	case request.GetPostgresRestoreVerified() != nil:
		value := request.GetPostgresRestoreVerified()
		if replay.postgresApplyStart == nil || replay.postgresVerified != nil || value.PointId != restore.PointId ||
			value.ContainerId != replay.postgresApplyStart.ContainerId || !proto.Equal(value.Evidence, restore.ExpectedEvidence) ||
			!backupCheckpointDigest(value.VerificationSha256) {
			return backupResumeHistoryInvalid()
		}
		replay.postgresVerified = value
		state.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		state.Checkpoint = &agentpb.BackupRestoreResume_PostgresRestoreVerified{PostgresRestoreVerified: value}
	case request.GetSourceCleanupCompleted() != nil:
		value := request.GetSourceCleanupCompleted()
		if replay.postgresVerified == nil || int(replay.postgresRecovered) != len(step.ConsumerServiceIds) ||
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

func (replay *BackupStepResumeReplay) postgresConsumerCheckpoint(value *agentpb.BackupPostgresServiceProgress) error {
	const idle = agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED
	recovering := replay.postgresVerified != nil
	index := int(replay.postgresStopped)
	if recovering {
		index = len(replay.step.ConsumerServiceIds) - 1 - int(replay.postgresRecovered)
	}
	if !validBackupPostgresService(value) || index < 0 || index >= len(replay.step.ConsumerServiceIds) ||
		value.ServiceCursor != uint32(index) || value.ServiceId != replay.step.ConsumerServiceIds[index] {
		return backupResumeHistoryInvalid()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range replay.authority.Services {
		if candidate.ServiceId == value.ServiceId {
			fact = candidate
			break
		}
	}
	if fact == nil {
		return backupResumeHistoryInvalid()
	}
	finished := false
	if fact.PriorRuntimeIntent.Kind != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
		if replay.postgresServicePhase != idle ||
			value.Phase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING {
			return backupResumeHistoryInvalid()
		}
		finished = true
	} else {
		phases := []agentpb.BackupServicePhase{
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT,
			agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED,
		}
		if recovering {
			phases = []agentpb.BackupServicePhase{
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT,
				agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY,
			}
		}
		previous, accepted := idle, false
		for index, phase := range phases {
			if replay.postgresServicePhase == previous && value.Phase == phase {
				accepted, finished = true, index == len(phases)-1
				break
			}
			previous = phase
		}
		if !accepted {
			return backupResumeHistoryInvalid()
		}
	}
	replay.postgresServicePhase = value.Phase
	if finished {
		replay.postgresServicePhase = idle
		if recovering {
			replay.postgresRecovered++
		} else {
			replay.postgresStopped++
		}
	}
	return nil
}
