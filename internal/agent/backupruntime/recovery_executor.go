package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) RunRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	identity workexecution.Identity, exchange Exchange, ownership *agentownership.Lease, staging *Staging,
) (workexecution.Attempt, error) {
	if executor == nil || ctx == nil || exchange == nil || ownership == nil {
		return workexecution.Attempt{}, errs.New(errs.KindInternal, "Backup prune recovery is not configured")
	}
	owned, attempt, source, err := executionplan.OpenBackupRecoveryAssignment(assignment, identity)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	savedAttempt, saved, err := executor.journal.LoadBackupRecovery(ctx, owned, identity)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved != nil && saved.Completion != nil {
		return *saved.Completion, nil
	}
	if saved != nil && saved.FailureReason != "" {
		return executor.failRecovery(ctx, owned, *savedAttempt, saved.FailureReason, exchange)
	}
	if saved != nil && len(saved.Recovery.Report) != 0 {
		// The Controller may already have settled this report. Do not request
		// another start grant before replaying the exact durable evidence.
		ack, err := exchange.RecoverBackup(ctx, &agentpb.BackupRecoveryRequest{Attempt: owned.Attempt, Report: saved.Recovery.Report})
		if err != nil {
			return workexecution.Attempt{}, err
		}
		return executor.journal.AcknowledgeBackupRecovery(ctx, *savedAttempt, ack)
	}
	start, err := exchange.StartAttempt(ctx, attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(start, attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	attempt.StartedAt = &started
	if savedAttempt != nil && (savedAttempt.StartedAt == nil || !savedAttempt.StartedAt.Equal(started)) {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Backup recovery start differs from its saved execution")
	}
	if _, err := executor.journal.PrepareBackupRecovery(ctx, attempt, source); err != nil {
		return workexecution.Attempt{}, err
	}
	bounded, cancel := context.WithDeadline(ctx, attempt.Deadline)
	defer cancel()
	if owned.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		if err := staging.AuthorizeRecovery(bounded, owned, attempt, ownership); err != nil {
			return executor.failRecovery(ctx, owned, attempt, executionplan.BackupExecutionFailed, exchange)
		}
	}
	if owned.Input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE &&
		owned.RestoreMode == agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION {
		if err := exchange.RegisterBackupRecoverySecrets(owned); err != nil {
			return workexecution.Attempt{}, err
		}
		defer exchange.ReleaseBackupSecrets(attempt.ID)
	}
	var report executionplan.BackupRecoveryReport
	switch owned.Input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		// Repeating executor exclusion is observation, never remote deletion.
		report, err = InspectPruneRecovery(bounded, ownership, attempt, owned.Input, source)
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		report, err = recoverCapture(bounded, owned, attempt, source, ownership, staging)
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		var delivery *checkpointdelivery.BackupRestoreRecovery
		delivery, err = checkpointdelivery.NewBackupRestoreRecovery(owned, attempt, executor.journal, exchange.BackupRecoveryCheckpoint)
		if err != nil {
			break
		}
		if err = delivery.Resume(bounded); err != nil {
			break
		}
		owned.Sources[0], err = delivery.Current(bounded)
		if err != nil {
			break
		}
		if owned.RestoreMode == agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION &&
			len(owned.Input.Steps) == 1 && owned.Input.Steps[0].GetRestore().GetConfig() != nil &&
			!restoreRecoveryHasCompletion(owned.Input.Steps[0], owned.Sources[0].GetRestore()) {
			if err = ownership.CheckRecovery(source.Forward, attempt); err != nil {
				break
			}
			var prepared *agentpb.BackupConfigRecoveryPrepareAck
			prepared, err = exchange.PrepareConfigRecovery(bounded, owned, 0)
			if err != nil {
				break
			}
			owned = prepared.Assignment
			cursor, readErr := delivery.Current(bounded)
			if readErr != nil {
				err = readErr
				break
			}
			progress := cursor.GetRestore().GetConfigProgress()
			if progress.GetTransfer() == nil && progress.GetMaterialization() == nil {
				err = delivery.Source().Config(bounded, &agentpb.BackupConfigCheckpoint{
					Checkpoint: &agentpb.BackupConfigCheckpoint_TransferCompleted{TransferCompleted: prepared.Completed},
				})
				if err != nil {
					break
				}
			}
		}
		step := owned.Input.Steps[0]
		if owned.RestoreMode == agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION &&
			step.GetRestore().GetConfig() != nil && !restoreRecoveryHasCompletion(step, owned.Sources[0].GetRestore()) {
			var cursor *agentpb.BackupStepResume
			cursor, err = executor.recoverConfigRestore(bounded, owned, attempt, source, ownership, staging, delivery.Source(), exchange)
			if err != nil {
				break
			}
			owned.Sources[0] = cursor
		}
		if owned.RestoreMode == agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION &&
			(step.GetRestore().GetPostgres() != nil || step.GetRestore().GetMysql() != nil) &&
			!restoreRecoveryHasCompletion(step, owned.Sources[0].GetRestore()) {
			owned.Sources[0], err = executor.continueDatabaseRestore(bounded, owned, attempt, source, ownership, staging, delivery.Source())
			if err != nil {
				break
			}
		}
		if owned.RestoreMode == agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION &&
			step.GetRestore().GetVolume() != nil && !restoreRecoveryHasCompletion(step, owned.Sources[0].GetRestore()) {
			owned.Sources[0], err = executor.recoverVolumeFilesystem(bounded, owned, attempt, source, ownership, staging, delivery.Source(), exchange)
			if err != nil {
				break
			}
			owned.Sources[0], err = executor.recoverVolumeServiceSet(bounded, owned, attempt.Executor, delivery.Source())
			if err != nil {
				break
			}
		}
		report, err = recoverRestore(bounded, owned, attempt, source, ownership, staging, executor.volumeRoot)
	default:
		err = errs.New(errs.KindStateConflict, "Restore cleanup requires captured mutation recovery")
	}
	if err != nil {
		reason := executionplan.BackupExecutionFailed
		if !time.Now().Before(attempt.Deadline) {
			reason = executionplan.BackupDeadlineExpired
		}
		return executor.failRecovery(ctx, owned, attempt, reason, exchange)
	}
	raw, err := executor.journal.SaveBackupRecoveryReport(ctx, attempt, report)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.RecoverBackup(ctx, &agentpb.BackupRecoveryRequest{Attempt: owned.Attempt, Report: raw})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeBackupRecovery(ctx, attempt, ack)
}

func (executor *Executor) failRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, reason executionplan.BackupFailureReason, exchange Exchange,
) (workexecution.Attempt, error) {
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	saved, err := executor.journal.PrepareBackupRecoveryFailure(reportCtx, attempt, reason, time.Now().UTC())
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved.Completion != nil {
		return *saved.Completion, nil
	}
	ack, err := exchange.FailBackup(reportCtx, &agentpb.BackupWorkFailureRequest{Attempt: assignment.Attempt, Reason: string(saved.FailureReason)})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeBackupRecoveryFailure(reportCtx, attempt, ack)
}
