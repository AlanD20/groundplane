package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) requireSources(
	assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity,
	staging *Staging,
) error {
	if err := executor.ownership.Check(); err != nil {
		return err
	}
	if assignment.Input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		return nil
	}
	if staging == nil {
		return errs.New(errs.KindStateConflict, "Backup source staging has not completed recovery")
	}
	if err := staging.AuthorizeVolumeResume(assignment, identity); err != nil {
		return err
	}
	if err := staging.AuthorizeSourceResume(assignment, identity); err != nil {
		return err
	}
	for _, step := range assignment.Input.Steps {
		if step.GetCapture().GetVolume() == nil && step.GetRestore().GetVolume() == nil &&
			step.GetCapture().GetConfig() == nil &&
			step.GetRestore().GetConfig() == nil &&
			step.GetCapture().GetPostgres() == nil &&
			step.GetCapture().GetMysql() == nil &&
			step.GetRestore().GetPostgres() == nil &&
			step.GetRestore().GetMysql() == nil {
			return errs.New(errs.KindStateConflict, "Backup source has no native execution procedure")
		}
	}
	return nil
}

func (executor *Executor) runSources(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	attempt workexecution.Attempt, exchange Exchange, staging *Staging,
) error {
	if assignment.Input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		return Prune(ctx, assignment, attempt, executor.journal, exchange)
	}
	if staging == nil {
		return errs.New(errs.KindStateConflict, "Backup source staging has not completed recovery")
	}
	consumers, err := NewBackupConsumerHost(executor.journal, executor.helper, attempt.Executor, executor.ownership)
	if err != nil {
		return err
	}
	delivery, err := checkpointdelivery.NewBackup(assignment, attempt, executor.journal, exchange.BackupCheckpoint)
	if err != nil {
		return err
	}
	if err := delivery.Resume(ctx); err != nil {
		return err
	}
	for ordinal, step := range assignment.Input.Steps {
		if err := executor.ownership.Check(); err != nil {
			return err
		}
		switch {
		case step.GetRestore().GetPostgres() != nil:
			if _, err := restorePostgres(ctx, assignment, attempt.Executor, uint32(ordinal), staging, consumers, delivery, exchange); err != nil {
				return err
			}
		case step.GetRestore().GetMysql() != nil:
			if _, err := restoreMySQL(ctx, assignment, attempt.Executor, uint32(ordinal), staging, consumers, delivery, exchange); err != nil {
				return err
			}
		case step.GetCapture().GetMysql() != nil:
			if err := captureMySQL(ctx, assignment, attempt.Executor, uint32(ordinal), staging, delivery, exchange); err != nil {
				return err
			}
		case step.GetCapture().GetPostgres() != nil:
			if err := capturePostgres(ctx, assignment, attempt.Executor, uint32(ordinal), staging, delivery, exchange); err != nil {
				return err
			}
		case step.GetRestore().GetConfig() != nil:
			if err := restoreConfig(ctx, assignment, attempt.Executor, uint32(ordinal), staging, executor.volumeRoot, executor.materializer, delivery, exchange); err != nil {
				return err
			}
		case step.GetCapture().GetConfig() != nil:
			if err := captureConfig(ctx, assignment, attempt.Executor, uint32(ordinal), staging, delivery, exchange); err != nil {
				return err
			}
		case step.GetCapture().GetVolume() != nil:
			if err := captureVolume(ctx, assignment, attempt.Executor, uint32(ordinal), staging, executor.volumeRoot, delivery, exchange); err != nil {
				return err
			}
		case step.GetRestore().GetVolume() != nil:
			if _, err := restoreVolume(ctx, assignment, attempt.Executor, uint32(ordinal), staging, executor.volumeRoot, consumers, delivery, exchange); err != nil {
				return err
			}
		default:
			return errs.New(errs.KindStateConflict, "Backup source has no native execution procedure")
		}
	}
	return nil
}
