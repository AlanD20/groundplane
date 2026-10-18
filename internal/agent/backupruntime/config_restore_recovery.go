package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) recoverConfigRestore(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, source executionplan.BackupWorkRecovery, ownership *agentownership.Lease,
	staging *Staging, publisher *checkpointdelivery.BackupSource, exchange SourceExchange,
) (cursor *agentpb.BackupStepResume, resultErr error) {
	if staging == nil || publisher == nil || ownership == nil {
		return nil, invalidDatabaseStaging()
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return nil, err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return nil, err
	}
	if cursor.GetRestore().GetConfigProgress().GetMaterialization() == nil {
		if cursor.GetRestore().GetConfigProgress().GetTransfer() == nil {
			return nil, errs.New(errs.KindStateConflict, "Config Restore recovery requires the captured publication receipt")
		}
		artifact, err := staging.retainedConfigRestore(ctx, assignment)
		if err != nil {
			return nil, err
		}
		if artifact == nil {
			staged, err := staging.RestoreRecovery(ctx, assignment, attempt.Executor, 0)
			if err != nil {
				return nil, err
			}
			artifact, err = downloadCapturedConfigRestore(ctx, assignment.Input, assignment.Attempt, 0, staged, exchange)
			if err != nil {
				return nil, err
			}
		}
		defer closeConfigRecoveryArtifact(ctx, artifact, &resultErr)
		if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
			return nil, err
		}
		proof, err := artifact.Materialize(ctx, attempt.TaskID, assignment.Input.Steps[0].StepId,
			executor.volumeRoot, executor.materializer)
		if err != nil {
			return nil, err
		}
		if err := publisher.Config(ctx, &agentpb.BackupConfigCheckpoint{
			Checkpoint: &agentpb.BackupConfigCheckpoint_MaterializationVerified{MaterializationVerified: proof},
		}); err != nil {
			return nil, err
		}
		if err := artifact.Close(ctx); err != nil {
			return nil, err
		}
	}
	binding, err := backupconfigtransfer.BindRecoverySource(assignment, attempt.Executor, 0,
		agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
	if err != nil {
		return nil, err
	}
	step := assignment.Input.Steps[0]
	journal, err := agentconfigjournal.Open(ctx, agentprotocol.StatePath+"/config-transfers/"+binding.TransferID, binding, step)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := journal.Close(); err != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, err)
		}
	}()
	if err := staging.cleanupCapturedSource(ctx, backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: step.GetRestore().PointId}); err != nil {
		return nil, err
	}
	if err := finishConfigRestore(ctx, step, publisher, journal); err != nil {
		return nil, err
	}
	return publisher.Current(ctx)
}
