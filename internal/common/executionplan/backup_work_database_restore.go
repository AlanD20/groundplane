package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AdvanceBackupDatabaseRestore carries bounded cumulative execution state in
// the saved cursor. No receipt-history replay or current desired-state read is
// needed to resume consumer recovery after the database has been changed.
func AdvanceBackupDatabaseRestore(assignment *agentpb.BackupWorkAssignment,
	request *agentpb.BackupWorkCheckpointRequest, authenticated workexecution.Identity,
) (*agentpb.BackupStepResume, error) {
	owned, _, err := OpenBackupWorkAssignment(assignment, authenticated)
	if err != nil {
		return nil, err
	}
	report, err := ValidateBackupWorkCheckpoint(request, owned.Input, authenticated)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(report.Attempt, owned.Attempt) {
		return nil, invalidBackupCheckpoint("database restore report belongs to another attempt")
	}
	return advanceDatabaseRestoreSource(owned.Input, owned.Sources, report)
}

func advanceDatabaseRestoreSource(input *agentpb.BackupWorkInput, sources []*agentpb.BackupStepResume,
	report *agentpb.BackupWorkCheckpointRequest,
) (*agentpb.BackupStepResume, error) {
	step := input.Steps[report.SourceOrdinal]
	current := sources[report.SourceOrdinal].GetRestore()
	restore := step.GetRestore()
	if current == nil ||
		(restore.GetPostgres() == nil && restore.GetMysql() == nil) ||
		current.CheckpointSequence == ^uint64(0) || report.CheckpointSequence != current.CheckpointSequence+1 ||
		!proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
		return nil, invalidBackupCheckpoint("database restore report does not follow saved progress")
	}
	next := proto.CloneOf(current)
	if next.DatabaseProgress == nil {
		if current.CheckpointSequence != 0 {
			return nil, invalidBackupCheckpoint("database restore cumulative progress is missing")
		}
		next.DatabaseProgress = &agentpb.BackupDatabaseRestoreProgress{}
	}
	var err error
	switch {
	case report.GetDatabaseConsumerRecovery() != nil:
		if report.GetDatabaseConsumerRecovery().PointId != restore.PointId || current.ApplyStart != nil ||
			current.MysqlApplyStart != nil || next.DatabaseProgress.ConsumersOnly ||
			(next.DatabaseProgress.PostgresObserved == nil && next.DatabaseProgress.MysqlObserved == nil) ||
			current.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION {
			return nil, invalidBackupCheckpoint(
				"consumer-only recovery requires proof that no database apply was authorized",
			)
		}
		next.DatabaseProgress.ConsumersOnly = true
		next.DatabaseProgress.ConsumerPhase = agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
		if next.DatabaseProgress.PostgresObserved != nil {
			next.Checkpoint = &agentpb.BackupRestoreResume_PostgresContainerObserved{
				PostgresContainerObserved: proto.CloneOf(next.DatabaseProgress.PostgresObserved),
			}
		} else {
			next.Checkpoint = &agentpb.BackupRestoreResume_MysqlContainerObserved{MysqlContainerObserved: proto.CloneOf(next.DatabaseProgress.MysqlObserved)}
		}
	case report.GetRestoreArtifactValidated() != nil:
		value := report.GetRestoreArtifactValidated()
		if current.CheckpointSequence != 0 || !backupResumeRestoreArtifactMatches(value, restore) {
			return nil, invalidBackupCheckpoint("database restore archive differs from the captured Recovery Point")
		}
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
		next.Checkpoint = &agentpb.BackupRestoreResume_ArtifactValidated{ArtifactValidated: value}
	case current.Checkpoint == nil || current.GetSourceCleanupCompleted() != nil:
		return nil, invalidBackupCheckpoint(
			"database restore must validate its archive and cannot continue after cleanup",
		)
	case report.GetSourceCleanupCompleted() != nil:
		value, progress := report.GetSourceCleanupCompleted(), next.DatabaseProgress
		if (!progress.ConsumersOnly && progress.PostgresVerified == nil && progress.MysqlVerified == nil) ||
			int(progress.RecoveredConsumers) != len(step.ConsumerServiceIds) ||
			value.PointId != restore.PointId || !proto.Equal(value.Evidence, restore.ExpectedEvidence) {
			return nil, invalidBackupCheckpoint(
				"database restore cleanup requires verified restore and recovered consumers",
			)
		}
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TERMINAL
		next.Checkpoint = &agentpb.BackupRestoreResume_SourceCleanupCompleted{SourceCleanupCompleted: value}
	case restore.GetPostgres() != nil:
		err = advancePostgresRestore(next, step, input.Services, report)
	case restore.GetMysql() != nil:
		err = advanceMySQLRestore(next, step, input.Services, report)
	}
	if err != nil {
		return nil, err
	}
	next.CheckpointSequence, next.PrecedingCheckpoint = report.CheckpointSequence, nil
	return &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Restore{Restore: next}}, nil
}
