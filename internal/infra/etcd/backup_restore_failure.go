package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareConfigRestoreFailure(ctx context.Context,
	runtime *BackupRuntimeRepository, current TaskAssignment, status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord, at time.Time,
) (TaskRecord, []etcdstore.Condition, []etcdstore.Mutation, error) {
	if status != taskjournal.TaskStatusFailed && status != taskjournal.TaskStatusAborted &&
		status != taskjournal.TaskStatusTimedOut {
		return TaskRecord{}, nil, nil, errs.New(errs.KindValidationFailed, "Restore failure status is invalid")
	}
	restored, err := runtime.GetBackupRestore(ctx, current.Task.Record.ID)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	if restored.Record.Point.SourceKind == backupruntime.BackupRuntimeSourceVolume {
		return repository.prepareVolumeRestoreFailure(ctx, runtime, current, status, result, at, restored)
	}
	if restored.Record.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
		return repository.prepareDatabaseRestoreFailure(ctx, runtime, current, status, result, at, restored)
	}
	// A publication send with a lost response makes the worker conservative,
	// but only the native intent can prove whether live effects began. This
	// terminal transaction fences that intent and the running Task together;
	// it also prevents an outstanding publication from starting afterward.
	// The delivery journal separately retains the worker's original Ack.
	result.ReconciliationRequired = restored.Record.MutationStarted
	at = backupTerminalTimestamp(at, restored.Record.UpdatedAt)
	taskPlan, err := repository.prepareBackupTaskTerminal(ctx, current, status, result, at)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer taskPlan.clear()
	var next backupruntime.BackupRestoreRecord
	if restored.Record.MutationStarted {
		next, err = backupruntime.RequireConfigRestoreRecovery(restored.Record, *taskPlan.record.FinishedAt)
	} else {
		next, err = backupruntime.FailConfigRestoreBeforeMutation(restored.Record, *taskPlan.record.FinishedAt)
	}
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	return repository.composeConfigRestoreTerminal(ctx, runtime, current.Task, restored, next, taskPlan)
}

// The Controller may prove a timeout preceded all live effects from native
// intent. Settlement repeats this compare; if publication wins, this result
// cannot release the operation lock or retire the assignment.
func (repository *TaskRepository) configRestoreTimeoutResult(ctx context.Context,
	current TaskAssignment,
) (taskjournal.TaskResultRecord, error) {
	assignment := current.Assignment.Record
	if current.Task.Record.Type != taskjournal.TaskRestore || assignment.BackupAuthorityFence == nil {
		return taskjournal.TaskResultRecord{}, errs.New(
			errs.KindStateConflict,
			"Restore timeout assignment is incomplete",
		)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{backupruntime.BackupRestoreKey(current.Task.Record.ID)}, Revision: current.Task.ReadRevision,
	})
	if err != nil {
		return taskjournal.TaskResultRecord{}, err
	}
	if read == nil || read.ReadRevision != current.Task.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return taskjournal.TaskResultRecord{}, errs.New(
			errs.KindStateConflict,
			"Restore timeout authority is unavailable",
		)
	}
	defer etcdstore.ClearValues(read.Values)
	restored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || restored.TaskID != current.Task.Record.ID ||
		restored.OperationID != current.Task.Record.OperationID ||
		restored.EnvironmentID != current.Task.Record.Owner.EnvironmentID ||
		(restored.Point.SourceKind != backupruntime.BackupRuntimeSourceConfig &&
			restored.Point.SourceKind != backupruntime.BackupRuntimeSourceVolume &&
			restored.Point.SourceKind != backupruntime.BackupRuntimeSourceAttach) ||
		restored.State == backupruntime.BackupRestoreQueued ||
		restored.State == backupruntime.BackupRestoreFailedSafe ||
		restored.State == backupruntime.BackupRestoreCompleted {
		return taskjournal.TaskResultRecord{}, errs.New(errs.KindStateConflict, "Restore timeout authority changed")
	}
	return taskjournal.TaskResultRecord{
		Kind: taskjournal.TaskResultBackup, Diagnostic: taskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch: assignment.ExecutionEpoch, AssignmentGeneration: assignment.BackupAuthorityFence.AssignmentGeneration,
		ReconciliationRequired: restored.MutationStarted,
	}, nil
}
