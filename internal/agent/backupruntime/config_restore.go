package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func restoreConfig(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging,
	volumeRoot string, materializer backupconfiguration.ConfigFileMaterializer,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (resultErr error) {
	binding, err := backupconfigtransfer.Bind(assignment, identity, ordinal, agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
	if err != nil {
		return err
	}
	step := assignment.Input.Steps[ordinal]
	publisher, err := delivery.Source(ordinal)
	if err != nil {
		return err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return err
	}
	resume := cursor.GetRestore()
	if resume == nil || materializer == nil {
		return errs.New(errs.KindStateConflict, "Config Restore runtime or cursor is unavailable")
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	journalPath := agentprotocol.StatePath + "/config-transfers/" + binding.TransferID
	absent, err := staging.SourceAbsent(assignment, identity, ordinal)
	if err != nil {
		return err
	}
	if resume.GetConfigProgress().GetSourceCleanupCompleted() != nil {
		if !absent {
			return errs.New(errs.KindStateConflict, "Completed Config Restore still has staged files")
		}
		return agentconfigjournal.ResumeCleanup(ctx, journalPath, binding, step, cursor)
	}
	journal, err := agentconfigjournal.Open(ctx, journalPath, binding, step)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := journal.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	if resume.GetConfigProgress().GetMaterialization() != nil {
		if !absent {
			staged, err := staging.Restore(ctx, assignment, identity, ordinal)
			if err != nil {
				return err
			}
			if err := staging.Cleanup(ctx, staged); err != nil {
				return err
			}
		}
		return finishConfigRestore(ctx, step, publisher, journal)
	}
	stream, err := exchange.RestoreConfigStream(ctx, binding)
	if err != nil {
		return err
	}
	staged, err := staging.Restore(ctx, assignment, identity, ordinal)
	if err != nil {
		return err
	}
	artifact, err := downloadConfigRestore(ctx, assignment, ordinal, staged, exchange)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if closeErr := artifact.Close(closeCtx); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	if resume.CheckpointSequence == 0 {
		if err := publisher.RestoreArtifactValidated(ctx, artifact.Evidence()); err != nil {
			return err
		}
	} else if resume.GetArtifactValidated() != nil && !proto.Equal(resume.GetArtifactValidated(), artifact.Evidence()) {
		return errs.New(errs.KindStateConflict, "Config Restore artifact differs from its acknowledged validation")
	}
	completed, err := artifact.SendRestore(ctx, stream, journal)
	if err != nil {
		return err
	}
	if resume.GetConfigProgress().GetTransfer() == nil {
		if err := publisher.Config(ctx, &agentpb.BackupConfigCheckpoint{
			Checkpoint: &agentpb.BackupConfigCheckpoint_TransferCompleted{TransferCompleted: completed},
		}); err != nil {
			return err
		}
	} else if !proto.Equal(resume.GetConfigProgress().GetTransfer(), completed) {
		return errs.New(errs.KindStateConflict, "Config Restore transfer differs from its acknowledged publication")
	}
	proof, err := artifact.Materialize(ctx, binding.Attempt.TaskID, step.StepId, volumeRoot, materializer)
	if err != nil {
		return err
	}
	if err := publisher.Config(ctx, &agentpb.BackupConfigCheckpoint{
		Checkpoint: &agentpb.BackupConfigCheckpoint_MaterializationVerified{MaterializationVerified: proof},
	}); err != nil {
		return err
	}
	if err := artifact.Close(ctx); err != nil {
		return err
	}
	if err := staging.Cleanup(ctx, staged); err != nil {
		return err
	}
	return finishConfigRestore(ctx, step, publisher, journal)
}

// Only the acknowledged cumulative cursor may retire the transfer journal.
// Cleanup cannot manufacture a sequence, publication proof or successful result.
func finishConfigRestore(ctx context.Context, step *agentpb.BackupStepAuthority,
	publisher *checkpointdelivery.BackupSource, journal *agentconfigjournal.Journal,
) error {
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return err
	}
	progress := cursor.GetRestore().GetConfigProgress()
	if progress.GetMaterialization() == nil {
		return errs.New(errs.KindStateConflict, "Config Restore cleanup requires its materialization proof")
	}
	if progress.GetSourceCleanupCompleted() == nil {
		if err := publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{
			PointId: step.GetRestore().PointId, Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence),
		}); err != nil {
			return err
		}
		cursor, err = publisher.Current(ctx)
		if err != nil {
			return err
		}
	}
	return journal.Cleanup(ctx, cursor)
}
