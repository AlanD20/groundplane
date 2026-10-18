package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateDatabaseWorkProgress(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority,
	services []*agentpb.BackupServiceFact,
) error {
	progress := state.GetDatabaseProgress()
	restore := step.GetRestore()
	invalid := func() error {
		return invalidBackupCheckpoint("saved database restore progress conflicts with captured input")
	}
	if state == nil || restore.GetPostgres() == nil && restore.GetMysql() == nil {
		if progress != nil {
			return invalid()
		}
		return nil
	}
	if state.CheckpointSequence == 0 {
		if progress != nil {
			return invalid()
		}
		return nil
	}
	if progress == nil || int(progress.StoppedConsumers) > len(step.ConsumerServiceIds) ||
		int(progress.RecoveredConsumers) > len(step.ConsumerServiceIds) ||
		(!progress.ConsumersOnly && progress.RecoveredConsumers > progress.StoppedConsumers) {
		return invalid()
	}
	observed, applied, verified := false, false, false
	if restore.GetPostgres() != nil {
		if progress.MysqlObserved != nil || progress.MysqlVerified != nil || state.MysqlApplyStart != nil {
			return invalid()
		}
		observation, apply, verification := progress.PostgresObserved, state.ApplyStart, progress.PostgresVerified
		observed, applied, verified = observation != nil, apply != nil, verification != nil
		if observed && !postgresObservationMatchesService(observation, step, services) {
			return invalid()
		}
		if applied && (!observed || apply.ContainerId != observation.ContainerId ||
			!bytes.Equal(apply.RepositoryDigest, observation.RepositoryDigest) ||
			!bytes.Equal(apply.ExpectedLabelsSha256, observation.ObservedLabelsSha256)) {
			return invalid()
		}
		if verified && (!applied || verification.ContainerId != apply.ContainerId || verification.PointId != restore.PointId ||
			!proto.Equal(verification.Evidence, restore.ExpectedEvidence) || !backupCheckpointDigest(verification.VerificationSha256)) {
			return invalid()
		}
	} else {
		if progress.PostgresObserved != nil || progress.PostgresVerified != nil || state.ApplyStart != nil {
			return invalid()
		}
		observation, apply, verification := progress.MysqlObserved, state.MysqlApplyStart, progress.MysqlVerified
		observed, applied, verified = observation != nil, apply != nil, verification != nil
		if observed && !mysqlObservationMatchesService(observation, step, services) {
			return invalid()
		}
		if applied && (!observed || apply.ContainerId != observation.ContainerId ||
			!bytes.Equal(apply.ImageReferenceSha256, observation.ImageReferenceSha256) ||
			!bytes.Equal(apply.ExpectedLabelsSha256, observation.ObservedLabelsSha256)) {
			return invalid()
		}
		if verified && (!applied || verification.ContainerId != apply.ContainerId || verification.PointId != restore.PointId ||
			!proto.Equal(verification.Evidence, restore.ExpectedEvidence) || !backupCheckpointDigest(verification.VerificationSha256)) {
			return invalid()
		}
	}
	if progress.ConsumersOnly && (!observed || applied || verified) {
		return invalid()
	}
	recovering := verified || progress.ConsumersOnly
	if !observed && (progress.StoppedConsumers != 0 || progress.ConsumerPhase != 0) ||
		applied && int(progress.StoppedConsumers) != len(step.ConsumerServiceIds) ||
		!recovering && progress.RecoveredConsumers != 0 ||
		recovering != (state.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY) {
		return invalid()
	}
	if state.GetSourceCleanupCompleted() != nil && int(progress.RecoveredConsumers) != len(step.ConsumerServiceIds) {
		return invalid()
	}
	return validateDatabaseConsumerCursor(state, step, recovering)
}

func validateDatabaseConsumerCursor(state *agentpb.BackupRestoreResume, step *agentpb.BackupStepAuthority, recovering bool) error {
	progress := state.DatabaseProgress
	phase := progress.ConsumerPhase
	if phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED {
		return nil
	}
	index := int(progress.StoppedConsumers)
	valid := phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT
	if recovering {
		index = len(step.ConsumerServiceIds) - 1 - int(progress.RecoveredConsumers)
		valid = phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT ||
			phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED ||
			phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTH_WAIT
	}
	if !valid || index < 0 || index >= len(step.ConsumerServiceIds) || state.Cursor.ServiceCursor != uint32(index) {
		return invalidBackupCheckpoint("saved database consumer cursor is invalid")
	}
	return nil
}
