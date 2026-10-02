package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) finishConfigRestore(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority, resume *agentpb.BackupRestoreResume, publisher *backupStepCheckpoint,
	journal *agentconfigjournal.Journal,
) error {
	progress := resume.GetConfigProgress()
	if progress.GetMaterialization() == nil {
		return invalidAgentStaging()
	}
	absent, err := pool.backupStaging.configStageAbsent(assignment.TaskID, step)
	if err != nil {
		return err
	}
	if !absent {
		// A resumed materialization receipt proves files, not stage cleanup.
		// Retire its exact prepared stage without another download or rendering.
		stage, _, _, err := pool.backupStaging.configRestoreStage(ctx, assignment.TaskID, step)
		if err != nil {
			return err
		}
		if err := stage.Cleanup(ctx); err != nil {
			return err
		}
		pool.backupStaging.retireConfigStage(assignment.TaskID, step, stage)
	}
	completion := proto.CloneOf(resume)
	if progress.SourceCleanupCompleted == nil {
		cleanup := &agentpb.BackupSourceCleanupCompleted{PointId: step.GetRestore().PointId,
			Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence)}
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
			Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{SourceCleanupCompleted: cleanup},
		}); err != nil {
			return err
		}
		completion.CheckpointSequence, completion.PrecedingCheckpoint = publisher.sequence, proto.CloneOf(
			publisher.fence,
		)
		completion.GetConfigProgress().SourceCleanupCompleted = cleanup
	}
	completion.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY
	return journal.Cleanup(ctx, &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Restore{Restore: completion}})
}
