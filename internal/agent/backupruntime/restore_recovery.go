package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func recoverRestore(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, source executionplan.BackupWorkRecovery,
	ownership *agentownership.Lease, staging *Staging, volumeRoot string,
) (executionplan.BackupRecoveryReport, error) {
	var zero executionplan.BackupRecoveryReport
	if staging == nil || ownership == nil || attempt.StartedAt == nil ||
		assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE || len(assignment.Sources) != 1 {
		return zero, invalidDatabaseStaging()
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return zero, err
	}
	resume := assignment.Sources[0].GetRestore()
	step := assignment.Input.Steps[0]
	completed := restoreRecoveryHasCompletion(step, resume)
	mutation := assignment.RestoreMode == agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION
	if completed && !mutation {
		return zero, errs.New(
			errs.KindStateConflict,
			"Restore completion conflicts with the Controller's no-mutation disposition",
		)
	}
	if !completed && mutation {
		return zero, errs.New(
			errs.KindStateConflict,
			"Restore has unfinished live changes; staging cleanup cannot settle it",
		)
	}
	if !completed && !restoreSourceAllowsUnmutatedCleanup(step, resume) {
		return zero, errs.New(
			errs.KindStateConflict,
			"Restore passed artifact validation; retain its live-state recovery evidence",
		)
	}
	// Archive validation can leave probe/list helpers behind even though no
	// database mutation was authorized. Inspect those before claiming cleanup.
	if err := retireSettledSourceHelpers(ctx, assignment.Input, step, completed && !resume.GetDatabaseProgress().GetConsumersOnly()); err != nil {
		return zero, err
	}
	if step.GetRestore().GetVolume() != nil && !completed {
		_, found, err := agentvolumejournal.Inspect(ctx, agentvolumejournal.AgentRoot, step.ExecutionId)
		if err != nil {
			return zero, err
		}
		if found {
			return zero, errs.New(
				errs.KindStateConflict,
				"Restore has a retained Volume journal; source cleanup is insufficient",
			)
		}
	}
	ids := backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: step.GetRestore().GetPointId()}
	if err := staging.cleanupCapturedSource(ctx, ids); err != nil {
		return zero, err
	}
	if step.GetRestore().GetConfig() != nil && completed {
		binding, err := backupconfigtransfer.BindRecoverySource(assignment, attempt.Executor, 0,
			agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
		if err != nil {
			return zero, err
		}
		if err := agentconfigjournal.ResumeCleanup(ctx, agentprotocol.StatePath+"/config-transfers/"+binding.TransferID,
			binding, step, assignment.Sources[0]); err != nil {
			return zero, err
		}
	}
	if step.GetRestore().GetVolume() != nil && completed {
		binding, err := backupvolumetransfer.BindRecoverySource(assignment, attempt.Executor, 0,
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
		if err != nil {
			return zero, err
		}
		if err := cleanupCompletedVolumeRestoreEvidence(ctx, binding, step, resume, volumeRoot); err != nil {
			return zero, err
		}
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return zero, err
	}
	report := executionplan.BackupRecoveryReport{AttemptID: attempt.ID, ForwardID: source.Forward.ID,
		Failure: source.Evidence, ExecutorStopped: true, Operation: workexecution.Restore, ObservedAt: time.Now().UTC(),
		Sources: []executionplan.BackupSourceRecoveryProof{
			{Ordinal: 0, PointID: ids.Point, StagingAbsent: true, HelperRetired: true},
		}}
	if err := report.ValidateInput(assignment.Input); err != nil {
		return zero, err
	}
	return report, report.Validate(attempt, source)
}

// Called only after the Controller's no-live-mutation disposition is checked.
// Database archive validation and container observation are read-only; an
// interruption there must not require a database apply to release the source.
func restoreSourceAllowsUnmutatedCleanup(step *agentpb.BackupStepAuthority, resume *agentpb.BackupRestoreResume) bool {
	if resume == nil || resume.ApplyStart != nil || resume.MysqlApplyStart != nil ||
		resume.GetConfigProgress() != nil || resume.GetVolumeProgress() != nil {
		return false
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION {
		return resume.GetPostgresServiceProgress() == nil && resume.GetMysqlServiceProgress() == nil
	}
	if resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION ||
		(step.GetRestore().GetPostgres() == nil && step.GetRestore().GetMysql() == nil) {
		return false
	}
	progress := resume.GetDatabaseProgress()
	if progress == nil || progress.ConsumersOnly || progress.RecoveredConsumers != 0 ||
		progress.PostgresVerified != nil || progress.MysqlVerified != nil {
		return false
	}
	// NOT_RUNNING records observation of an already-stopped consumer, not a
	// stop command. Any authorized stop or restart still requires recovery.
	return progress.ConsumerPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED ||
		progress.ConsumerPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING
}

func restoreRecoveryHasCompletion(step *agentpb.BackupStepAuthority, resume *agentpb.BackupRestoreResume) bool {
	if resume == nil || step.GetRestore() == nil {
		return false
	}
	if step.GetRestore().GetVolume() != nil {
		return volumeRestoreCheckpointProvesVerified(resume, len(step.ConsumerServiceIds))
	}
	cleanup := resume.GetSourceCleanupCompleted()
	if step.GetRestore().GetConfig() != nil {
		progress := resume.GetConfigProgress()
		if progress.GetMaterialization() == nil {
			return false
		}
		cleanup = progress.GetSourceCleanupCompleted()
	}
	return cleanup != nil && cleanup.PointId == step.GetRestore().PointId &&
		proto.Equal(cleanup.Evidence, step.GetRestore().ExpectedEvidence)
}
