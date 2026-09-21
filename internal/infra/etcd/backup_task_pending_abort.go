package etcd

import (
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *TaskRepository) abortPendingBackupTask(
	ctx context.Context,
	taskID string,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
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
		if current.Record.Status == taskjournal.TaskStatusAborted {
			if err := repository.validateBackupTaskTerminalReplay(
				ctx, current, taskjournal.TaskStatusAborted, nil, nil,
			); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			return current, nil
		}
		if current.Record.Status != taskjournal.TaskStatusPending ||
			(current.Record.Type != taskjournal.TaskBackup && current.Record.Type != taskjournal.TaskBackupPrune) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"only a pending Backup Task can be aborted before assignment",
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
		case taskjournal.TaskBackup:
			run, getErr := runtime.GetBackupRun(ctx, taskID)
			if getErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, getErr
			}
			if err := validateBackupRunTaskBinding(current.Record, run.Record); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, run.Record.UpdatedAt)
			next, transitionErr := backupRunForTaskTerminal(
				run.Record, taskjournal.TaskStatusAborted, effectiveAt,
			)
			if transitionErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, transitionErr
			}
			taskPlan, prepareErr := repository.preparePendingBackupTaskTerminal(
				ctx, current, effectiveAt,
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
				ctx, current, effectiveAt,
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
		transaction, err := runtime.transact(ctx, conditions, mutations)
		clearBackupRuntimeMutations(mutations)
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
	terminalAt time.Time,
) (backupTaskTerminalPlan, error) {
	task := current.Record
	if current.Revision <= 0 || current.ReadRevision <= 0 ||
		(task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskBackupPrune) ||
		task.Executor != taskjournal.TaskExecutorAgent || task.Status != taskjournal.TaskStatusPending {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindValidationFailed,
			"pending Backup Task terminal identity is invalid",
		)
	}
	terminal, err := transitionTaskStatus(task, taskjournal.TaskStatusPending, taskjournal.TaskStatusAborted, terminalAt)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	terminalAt = *terminal.FinishedAt
	transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
		task, taskjournal.TaskStatusAborted, terminalAt,
	)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	defer clear(taskRetentionValue)
	keys := []string{
		taskjournal.TaskActiveOperationKey(task.OperationID), markerKey,
		taskjournal.TaskQueueKey(task.Executor, task.ID), retentionKey, taskRetentionKey,
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: keys, Revision: current.ReadRevision,
	})
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	if companions == nil || companions.ReadRevision != current.ReadRevision ||
		len(companions.Values) != len(keys) || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] == nil ||
		companions.Values[3] != nil || companions.Values[4] != nil {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindInternal,
			"pending Backup Task lifecycle records are inconsistent",
		)
	}
	defer etcdstore.ClearValues(companions.Values)
	queuedTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[2].Value)
	if err != nil || queuedTaskID != task.ID ||
		validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]) != nil {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindInternal,
			"pending Backup Task companions do not match",
		)
	}
	transitionedMarker, err = hydrateTerminalTaskMarker(
		transitionedMarker, companions.Values[1].Value,
	)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	terminalValue, err := encodeTaskRecord(terminal)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(transitionedMarker)
	clear(transitionedMarker.Intent.Ciphertext)
	clear(transitionedMarker.Response.Body)
	if err != nil {
		clear(terminalValue)
		return backupTaskTerminalPlan{}, err
	}
	retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		clear(terminalValue)
		clear(markerValue)
		return backupTaskTerminalPlan{}, errs.Wrap(errs.KindInternal, err)
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: current.Revision},
		{Key: keys[0], ModRevision: companions.Values[0].ModRevision},
		{Key: keys[1], ModRevision: companions.Values[1].ModRevision},
		{Key: keys[2], ModRevision: companions.Values[2].ModRevision},
		{Key: keys[3]}, {Key: keys[4]},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: terminalValue},
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: markerValue},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
		{Type: etcdstore.MutationPut, Key: keys[3], Value: retentionValue},
		{Type: etcdstore.MutationPut, Key: keys[4], Value: append([]byte(nil), taskRetentionValue...)},
	}
	return backupTaskTerminalPlan{conditions: conditions, mutations: mutations, record: terminal}, nil
}
