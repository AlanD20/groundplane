package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) TransitionBackupRun(
	ctx context.Context,
	authority backupruntime.BackupAssignmentInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	if backupruntime.TerminalBackupRunState(next.State) {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup run requires atomic Task completion",
		)
	}
	if err := backupruntime.ValidateBackupRunTransition(
		current.Record,
		next,
		backupruntime.BackupRunTransitionOrdinary,
	); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	if backupruntime.BackupRunTransitionRequiresCheckpoint(current.Record, next) {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup source transition requires a checkpoint",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, &authority, nil)
}

func (repository *BackupRuntimeRepository) CheckpointBackupRun(
	ctx context.Context,
	checkpoint backupruntime.BackupCheckpointInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	if backupruntime.TerminalBackupRunState(next.State) || backupruntime.ValidateBackupRunTransition(
		current.Record,
		next,
		backupruntime.BackupRunTransitionOrdinary,
	) != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"checkpointed backup run transition is invalid",
		)
	}
	ordinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
	if !changed || !backupruntime.BackupRunCheckpointMatchesTransition(
		checkpoint.Payload,
		current.Record.Sources[ordinal],
		next.Sources[ordinal],
	) {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup run checkpoint does not match its source transition",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, nil, &checkpoint)
}

func backupRunCheckpointBinding(run backupruntime.BackupRunRecord, ordinal uint32) backupCheckpointBinding {
	if int(ordinal) >= len(run.Sources) {
		return backupCheckpointBinding{}
	}
	return backupCheckpointBinding{
		taskType: taskjournal.TaskBackup, ordinal: ordinal, pointID: run.Sources[ordinal].RecoveryPointID,
	}
}
