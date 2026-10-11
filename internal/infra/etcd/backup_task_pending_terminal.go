package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *TaskRepository) terminalizePendingBackupTask(
	ctx context.Context,
	taskID string,
	status taskjournal.TaskStatus,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	if status != taskjournal.TaskStatusAborted && status != taskjournal.TaskStatusTimedOut {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"pending backup terminal status is invalid",
		)
	}
	runtime, err := newBackupRuntimeRepository(repository.store)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	conflicts := 0
	for {
		current, err := repository.GetTask(ctx, taskID)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if current.Record.Status == status {
			if err := repository.validateBackupTaskTerminalReplay(
				ctx, current, status, nil, nil,
			); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			return current, nil
		}
		if current.Record.Status != taskjournal.TaskStatusPending ||
			(current.Record.Type != taskjournal.TaskBackup && current.Record.Type != taskjournal.TaskBackupPrune && current.Record.Type != taskjournal.TaskRestore) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"only a pending Backup Task can finish before assignment",
			)
		}
		if status == taskjournal.TaskStatusTimedOut &&
			terminalAt.Before(current.Record.CreatedAt.Add(time.Duration(current.Record.TimeoutSeconds)*time.Second)) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"pending backup deadline has not expired",
			)
		}
		effectiveAt, err := nextTaskControllerTimestamp(current.Record.UpdatedAt, terminalAt)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		var conditions []etcdstore.Condition
		var mutations []etcdstore.Mutation
		var terminal TaskRecord
		switch current.Record.Type {
		case taskjournal.TaskRestore:
			terminal, conditions, mutations, err = repository.preparePendingConfigRestoreTerminal(
				ctx, runtime, current, status, effectiveAt)
		case taskjournal.TaskBackup:
			run, getErr := runtime.GetBackupRun(ctx, taskID)
			if getErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, getErr
			}
			if err := ValidateBackupRunTaskBinding(current.Record, run.Record); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, run.Record.UpdatedAt)
			next, transitionErr := backupRunForTaskTerminal(
				run.Record, status, effectiveAt,
			)
			if transitionErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, transitionErr
			}
			taskPlan, prepareErr := repository.preparePendingBackupTaskTerminal(
				ctx, current, status, effectiveAt,
			)
			if prepareErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			runPlan, prepareErr := runtime.prepareBackupRunTerminal(ctx, run, next)
			if prepareErr != nil {
				taskPlan.clear()
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			receiptPlan, prepareErr := prepareBackupRunTerminalReceipt(
				current, taskPlan.record, runPlan.record,
			)
			if prepareErr != nil {
				taskPlan.clear()
				runPlan.clear()
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			terminal = taskPlan.record
			conditions, mutations, err = composeBackupRunTerminalTransaction(
				taskPlan, runPlan, receiptPlan,
			)
			taskPlan.clear()
			runPlan.clear()
			receiptPlan.clear()
		case taskjournal.TaskBackupPrune:
			dispatch, prunes, loadErr := repository.loadBackupPruneTerminalAuthority(
				ctx, current.Record,
			)
			if loadErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, loadErr
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, dispatch.Record.CreatedAt)
			for _, prune := range prunes {
				effectiveAt = backupTerminalTimestamp(effectiveAt, prune.Record.UpdatedAt)
			}
			taskPlan, prepareErr := repository.preparePendingBackupTaskTerminal(
				ctx, current, status, effectiveAt,
			)
			if prepareErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			prunePlan, prepareErr := runtime.prepareBackupPruneFailure(
				ctx, dispatch, prunes, effectiveAt,
			)
			if prepareErr != nil {
				taskPlan.clear()
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			receiptPlan, prepareErr := prepareBackupPruneTerminalReceipt(
				current, taskPlan.record, dispatch.Record, prunes,
			)
			if prepareErr != nil {
				taskPlan.clear()
				prunePlan.clear()
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			terminal = taskPlan.record
			conditions, mutations, err = composeBackupPruneTerminalTransaction(
				taskPlan, prunePlan, receiptPlan,
			)
			taskPlan.clear()
			prunePlan.clear()
			receiptPlan.clear()
		}
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		transaction, err := runtime.TransactRuntime(ctx, conditions, mutations)
		etcdstore.ClearMutationValues(mutations)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		etcdstore.ClearValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			continue
		}
		return etcdstore.Versioned[TaskRecord]{
			Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
}

func (repository *TaskRepository) preparePendingBackupTaskTerminal(
	ctx context.Context,
	current etcdstore.Versioned[TaskRecord],
	status taskjournal.TaskStatus,
	terminalAt time.Time,
) (backupTaskTerminalPlan, error) {
	task := current.Record
	if (task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskBackupPrune && task.Type != taskjournal.TaskRestore) ||
		(status != taskjournal.TaskStatusAborted && status != taskjournal.TaskStatusTimedOut) {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindValidationFailed,
			"pending Backup Task terminal identity is invalid",
		)
	}
	plan, err := repository.preparePendingTaskTerminal(ctx, current, status, terminalAt, nil)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	return backupTaskTerminalPlan(plan), nil
}
