package etcd

import (
	"bytes"
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// acknowledgeBackupTask routes the public Agent acknowledgement through the
// Backup domain's terminal plan. Task state, run or prune state, assignment
// cleanup, exclusions, dispatch, and the Environment lock therefore commit at
// one revision rather than exposing a generic Task-only terminal gap.
func (repository *TaskRepository) acknowledgeBackupTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus taskjournal.TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	runtime, err := newBackupRuntimeRepository(repository.store)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	conflicts := 0
	for {
		current, assigned, err := repository.loadBackupTaskAssignment(
			ctx, agentID, agentGeneration, taskID, assignmentID,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if !assigned {
			expectedAssignment := TaskTerminalAssignmentRecord{
				AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
			}
			if err := repository.validateBackupTaskTerminalReplay(
				ctx, current.Task, terminalStatus, &result, &expectedAssignment,
			); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			return current.Task, nil
		}

		effectiveAt, err := nextTaskControllerTimestamp(current.Task.Record.UpdatedAt, terminalAt)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		var conditions []etcdstore.Condition
		var mutations []etcdstore.Mutation
		var terminal TaskRecord
		switch current.Task.Record.Type {
		case taskjournal.TaskBackup:
			run, getErr := runtime.GetBackupRun(ctx, taskID)
			if getErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, getErr
			}
			if err := validateBackupRunTaskBinding(current.Task.Record, run.Record); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, run.Record.UpdatedAt)
			next, transitionErr := backupRunForTaskTerminal(run.Record, terminalStatus, effectiveAt)
			if transitionErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, transitionErr
			}
			taskPlan, prepareErr := repository.prepareBackupTaskTerminal(
				ctx, current, terminalStatus, result, effectiveAt,
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
				current.Task, taskPlan.record, runPlan.record,
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
				ctx, current.Task.Record,
			)
			if loadErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, loadErr
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, dispatch.Record.CreatedAt)
			for _, prune := range prunes {
				effectiveAt = backupTerminalTimestamp(effectiveAt, prune.Record.UpdatedAt)
			}
			taskPlan, prepareErr := repository.prepareBackupTaskTerminal(
				ctx, current, terminalStatus, result, effectiveAt,
			)
			if prepareErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			var prunePlan backupPruneTransactionPlan
			if terminalStatus == taskjournal.TaskStatusCompleted {
				prunePlan, prepareErr = runtime.prepareBackupPruneCompletion(ctx, dispatch, prunes)
			} else {
				prunePlan, prepareErr = runtime.prepareBackupPruneFailure(
					ctx, dispatch, prunes, effectiveAt,
				)
			}
			if prepareErr != nil {
				taskPlan.clear()
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			receiptPlan, prepareErr := prepareBackupPruneTerminalReceipt(
				current.Task, taskPlan.record, dispatch.Record, prunes,
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
		default:
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"backup Task terminal dispatch received an ordinary Task",
			)
		}
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		transaction, err := runtime.transact(ctx, conditions, mutations)
		clearBackupRuntimeMutations(mutations)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		clearKeyValues(transaction.FailureReads)
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

func (repository *TaskRepository) loadBackupTaskAssignment(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
) (TaskAssignment, bool, error) {
	claimKey := taskAssignmentKey(agentID, taskID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskKey(taskID), claimKey, taskAssignmentIndexKey(taskID),
	}})
	if err != nil {
		return TaskAssignment{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 3 || read.Values[0] == nil {
		return TaskAssignment{}, false, errs.New(errs.KindInternal, "backup Task assignment read is incomplete")
	}
	defer clearKeyValues(read.Values)
	task, err := decodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != taskID || task.Executor != taskjournal.TaskExecutorAgent ||
		(task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskBackupPrune) {
		return TaskAssignment{}, false, errs.New(errs.KindInternal, "backup Task assignment is corrupt")
	}
	current := TaskAssignment{Task: etcdstore.Versioned[TaskRecord]{
		Record: task, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}}
	if read.Values[1] == nil {
		if read.Values[2] != nil {
			return TaskAssignment{}, false, errs.New(
				errs.KindStateConflict,
				"backup Task assignment identity changed",
			)
		}
		return current, false, nil
	}
	if read.Values[2] == nil || read.Values[1].ModRevision != read.Values[2].ModRevision ||
		!bytes.Equal(read.Values[1].Value, read.Values[2].Value) {
		return TaskAssignment{}, false, errs.New(errs.KindInternal, "backup Task assignment copies differ")
	}
	assignment, err := decodeTaskAssignment(read.Values[1].Value)
	if err != nil || assignment.TaskID != taskID || assignment.Executor != taskjournal.TaskExecutorAgent ||
		assignment.AssignmentID != assignmentID || assignment.AgentID != agentID ||
		assignment.AgentGeneration != agentGeneration || assignment.ClaimedTaskRevision >= read.Values[1].ModRevision ||
		task.Status != taskjournal.TaskStatusRunning || task.StartedAt == nil ||
		!task.StartedAt.Equal(assignment.AssignedAt) ||
		!assignment.Deadline.Equal(assignment.AssignedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
		read.Values[0].ModRevision < read.Values[1].ModRevision || task.idempotencyMarker == nil {
		return TaskAssignment{}, false, errs.New(
			errs.KindStateConflict,
			"backup Task assignment identity changed",
		)
	}
	current.Assignment = etcdstore.Versioned[TaskAssignmentRecord]{
		Record: assignment, Revision: read.Values[1].ModRevision, ReadRevision: read.ReadRevision,
	}
	return current, true, nil
}

func (repository *TaskRepository) loadBackupPruneTerminalAuthority(
	ctx context.Context,
	task TaskRecord,
) (
	etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
	[]etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	error,
) {
	taskID := task.ID
	dispatchRead, err := repository.store.Get(ctx, backupruntime.BackupRecoveryPointPruneDispatchKey(taskID))
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, err
	}
	if dispatchRead == nil || dispatchRead.Entry == nil || dispatchRead.ReadRevision <= 0 {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup prune dispatch is missing for its active Task",
		)
	}
	defer clear(dispatchRead.Entry.Value)
	dispatchRecord, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(dispatchRead.Entry.Value)
	if err != nil || dispatchRecord.TaskID != taskID {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	if err := validateBackupPruneTaskBinding(task, dispatchRecord); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, err
	}
	dispatch := etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{
		Record: dispatchRecord, Revision: dispatchRead.Entry.ModRevision,
		ReadRevision: dispatchRead.ReadRevision,
	}
	keys := make([]string, len(dispatchRecord.RecoveryPointIDs))
	for index, pointID := range dispatchRecord.RecoveryPointIDs {
		keys[index] = backupruntime.BackupRecoveryPointPruneKey(pointID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup prune authority read is incomplete",
		)
	}
	defer clearKeyValues(read.Values)
	prunes := make([]etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], len(keys))
	for index, value := range read.Values {
		if value == nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, errs.New(
				errs.KindInternal,
				"backup prune authority is missing for its active Task",
			)
		}
		record, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(value.Value)
		if decodeErr != nil || record.Point.ID != dispatchRecord.RecoveryPointIDs[index] ||
			record.Point.EnvironmentID != dispatchRecord.EnvironmentID ||
			record.OperationID != dispatchRecord.OperationID || record.TaskID != taskID ||
			(record.State != backupruntime.BackupPruneAssigned && record.State != backupruntime.BackupPruneVerifiedAbsent) {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{}, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		prunes[index] = etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: read.ReadRevision,
		}
	}
	return dispatch, prunes, nil
}

func backupRunForTaskTerminal(
	current backupruntime.BackupRunRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
) (backupruntime.BackupRunRecord, error) {
	next := current
	next.Sources = append([]backupruntime.BackupRunSourceAttemptRecord(nil), current.Sources...)
	next.UpdatedAt = terminalAt
	switch terminalStatus {
	case taskjournal.TaskStatusCompleted:
		next.State = backupruntime.BackupRunCompleted
		return next, nil
	case taskjournal.TaskStatusFailed:
		next.State = backupruntime.BackupRunFailed
	case taskjournal.TaskStatusAborted:
		next.State = backupruntime.BackupRunAborted
	case taskjournal.TaskStatusTimedOut:
		next.State = backupruntime.BackupRunTimedOut
	default:
		return backupruntime.BackupRunRecord{}, errs.New(
			errs.KindValidationFailed,
			"backup Task terminal status is invalid",
		)
	}
	boundary := -1
	for index := range next.Sources {
		if next.Sources[index].State != backupruntime.BackupSourceAttemptSucceeded {
			boundary = index
			break
		}
	}
	if boundary < 0 {
		return backupruntime.BackupRunRecord{}, errs.New(
			errs.KindStateConflict,
			"non-success Backup acknowledgement has no active source",
		)
	}
	source := &next.Sources[boundary]
	if source.State == backupruntime.BackupSourceAttemptStaged &&
		(source.Phase == backupruntime.BackupSourcePhaseUpload ||
			source.Phase == backupruntime.BackupSourcePhaseHeadVerification ||
			source.Phase == backupruntime.BackupSourcePhasePointCommit) {
		source.State = backupruntime.BackupSourceAttemptOrphaned
	} else if source.State != backupruntime.BackupSourceAttemptOrphaned {
		source.State = backupruntime.BackupSourceAttemptFailed
	}
	switch terminalStatus {
	case taskjournal.TaskStatusAborted:
		source.FailureCode = backupruntime.BackupFailureAborted
	case taskjournal.TaskStatusTimedOut:
		source.FailureCode = backupruntime.BackupFailureTimedOut
	default:
		failureCode, err := backupFailureCodeForPhase(source.Phase)
		if err != nil {
			return backupruntime.BackupRunRecord{}, err
		}
		source.FailureCode = failureCode
	}
	for index := boundary + 1; index < len(next.Sources); index++ {
		next.Sources[index].State = backupruntime.BackupSourceAttemptUnstarted
		next.Sources[index].Phase = backupruntime.BackupSourcePhaseCapture
		next.Sources[index].SizeBytes = 0
		next.Sources[index].SHA256 = ""
		next.Sources[index].FailureCode = ""
	}
	return next, nil
}

func validateBackupRunTaskBinding(task TaskRecord, run backupruntime.BackupRunRecord) error {
	if task.Type != taskjournal.TaskBackup || task.ID != run.TaskID || task.OperationID != run.OperationID ||
		task.Owner.EnvironmentID == "" || task.Owner.EnvironmentID != run.EnvironmentID ||
		task.Target != run.EnvironmentID || !task.CreatedAt.Equal(run.CreatedAt) ||
		task.RetryOf != run.RetryOfTaskID {
		return errs.New(errs.KindInternal, "backup task and run identity differ")
	}
	return nil
}

func validateBackupPruneTaskBinding(
	task TaskRecord,
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
) error {
	if task.Type != taskjournal.TaskBackupPrune || task.ID != dispatch.TaskID ||
		task.OperationID != dispatch.OperationID || task.Owner.EnvironmentID == "" ||
		task.Owner.EnvironmentID != dispatch.EnvironmentID || task.Target != dispatch.EnvironmentID ||
		!task.CreatedAt.Equal(dispatch.CreatedAt) {
		return errs.New(errs.KindInternal, "backup prune Task and dispatch identity differ")
	}
	return nil
}

func backupFailureCodeForPhase(phase backupruntime.BackupSourceAttemptPhase) (backupruntime.BackupFailureCode, error) {
	switch phase {
	case BackupSourcePhaseCapture:
		return backupruntime.BackupFailureCapture, nil
	case BackupSourcePhaseStaging:
		return backupruntime.BackupFailureStaging, nil
	case BackupSourcePhaseUpload:
		return backupruntime.BackupFailureUpload, nil
	case BackupSourcePhaseHeadVerification:
		return backupruntime.BackupFailureHeadVerification, nil
	case BackupSourcePhasePointCommit:
		return backupruntime.BackupFailurePointCommit, nil
	case BackupSourcePhaseCleanup:
		return backupruntime.BackupFailureCleanup, nil
	case BackupSourcePhaseRetention:
		return backupruntime.BackupFailureRetention, nil
	default:
		return "", errs.New(errs.KindInternal, "backup source phase is corrupt")
	}
}

func backupTerminalTimestamp(supplied time.Time, previous time.Time) time.Time {
	if supplied.After(previous) {
		return supplied
	}
	return previous.Add(time.Nanosecond)
}

func (repository *TaskRepository) validateBackupTaskTerminalReplay(
	ctx context.Context,
	task etcdstore.Versioned[TaskRecord],
	terminalStatus taskjournal.TaskStatus,
	result *TaskResultRecord,
	assignment *TaskTerminalAssignmentRecord,
) error {
	if task.Record.Status != terminalStatus ||
		((result == nil) != (task.Record.Result == nil)) ||
		(result != nil && !taskResultsEqual(*result, *task.Record.Result)) ||
		((assignment == nil) != (task.Record.TerminalAssignment == nil)) ||
		(assignment != nil && *assignment != *task.Record.TerminalAssignment) {
		return errs.New(errs.KindStateConflict, "backup Task terminal acknowledgement changed")
	}
	if err := repository.validateTaskRetentionReplay(ctx, task.Record, task.ReadRevision); err != nil {
		return err
	}
	// Rationale: a retained terminal receipt, not compactable MVCC history or
	// perpetual ownership of released domain keys, is the replay authority.
	return repository.validateBackupTerminalReceiptReplay(ctx, task)
}

func backupRunStateForTaskStatus(status taskjournal.TaskStatus) (backupruntime.BackupRunState, error) {
	switch status {
	case taskjournal.TaskStatusCompleted:
		return backupruntime.BackupRunCompleted, nil
	case taskjournal.TaskStatusFailed:
		return backupruntime.BackupRunFailed, nil
	case taskjournal.TaskStatusAborted:
		return backupruntime.BackupRunAborted, nil
	case taskjournal.TaskStatusTimedOut:
		return backupruntime.BackupRunTimedOut, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "backup Task terminal status is invalid")
	}
}
