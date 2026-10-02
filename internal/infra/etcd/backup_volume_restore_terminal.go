package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareVolumeRestoreSuccessfulTerminal(ctx context.Context,
	runtime *BackupRuntimeRepository, current TaskAssignment, result taskjournal.TaskResultRecord, at time.Time,
	restored etcdstore.Versioned[backupruntime.BackupRestoreRecord],
) (TaskRecord, []etcdstore.Condition, []etcdstore.Mutation, error) {
	at = backupTerminalTimestamp(at, restored.Record.UpdatedAt)
	taskPlan, err := repository.prepareBackupTaskTerminal(ctx, current, taskjournal.TaskStatusCompleted, result, at)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer taskPlan.clear()
	next, err := backupruntime.CompleteVolumeRestore(restored.Record, *taskPlan.record.FinishedAt)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	return repository.composeVolumeRestoreTerminal(ctx, runtime, current.Task, restored, next, taskPlan)
}

func (repository *TaskRepository) preparePendingVolumeRestoreTerminal(ctx context.Context,
	runtime *BackupRuntimeRepository, current etcdstore.Versioned[TaskRecord],
	status taskjournal.TaskStatus, at time.Time,
	restored etcdstore.Versioned[backupruntime.BackupRestoreRecord],
) (TaskRecord, []etcdstore.Condition, []etcdstore.Mutation, error) {
	if restored.Record.State != backupruntime.BackupRestoreQueued {
		return TaskRecord{}, nil, nil, errs.New(errs.KindStateConflict, "pending Volume Restore has execution progress")
	}
	at = backupTerminalTimestamp(at, restored.Record.UpdatedAt)
	taskPlan, err := repository.preparePendingBackupTaskTerminal(ctx, current, status, at)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer taskPlan.clear()
	next, err := backupruntime.FailVolumeRestoreBeforeMutation(restored.Record, *taskPlan.record.FinishedAt)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	return repository.composeVolumeRestoreTerminal(ctx, runtime, current, restored, next, taskPlan)
}

func (repository *TaskRepository) prepareVolumeRestoreFailure(ctx context.Context,
	runtime *BackupRuntimeRepository, current TaskAssignment, status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord, at time.Time,
	restored etcdstore.Versioned[backupruntime.BackupRestoreRecord],
) (TaskRecord, []etcdstore.Condition, []etcdstore.Mutation, error) {
	// The native intent, not the worker's last observed acknowledgement, proves
	// whether a live Service or filesystem effect may have begun.
	result.ReconciliationRequired = restored.Record.MutationStarted
	at = backupTerminalTimestamp(at, restored.Record.UpdatedAt)
	taskPlan, err := repository.prepareBackupTaskTerminal(ctx, current, status, result, at)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer taskPlan.clear()
	var next backupruntime.BackupRestoreRecord
	if restored.Record.MutationStarted {
		next, err = backupruntime.RequireVolumeRestoreRecovery(restored.Record, *taskPlan.record.FinishedAt)
	} else {
		next, err = backupruntime.FailVolumeRestoreBeforeMutation(restored.Record, *taskPlan.record.FinishedAt)
	}
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	return repository.composeVolumeRestoreTerminal(ctx, runtime, current.Task, restored, next, taskPlan)
}

func (repository *TaskRepository) composeVolumeRestoreTerminal(ctx context.Context,
	runtime *BackupRuntimeRepository, current etcdstore.Versioned[TaskRecord],
	restored etcdstore.Versioned[backupruntime.BackupRestoreRecord], next backupruntime.BackupRestoreRecord,
	taskPlan backupTaskTerminalPlan,
) (TaskRecord, []etcdstore.Condition, []etcdstore.Mutation, error) {
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(next.EnvironmentID, next.TaskID)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	keys := []string{backupruntime.BackupRestoreKey(next.TaskID), membership,
		backupruntime.BackupExecutionPlanKey(next.TaskID), blueprints.EnvironmentBlueprintHeadKey(next.EnvironmentID)}
	read, err := runtime.ReadFixedKeys(ctx, keys, restored.ReadRevision)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != len(keys) {
		return TaskRecord{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Volume Restore terminal authority is incomplete",
		)
	}
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return TaskRecord{}, nil, nil, errs.New(
				errs.KindStateConflict,
				"Volume Restore terminal authority is incomplete",
			)
		}
	}
	plan, err := backupruntime.DecodeBackupExecutionPlan(read.Values[2].Value)
	if err != nil || next.CurrentTarget.Volume == nil || read.Values[0].ModRevision != restored.Revision ||
		read.Values[1].Version != 1 || string(read.Values[1].Value) != next.TaskID ||
		read.Values[2].Version != 1 || backupruntime.ValidateVolumeRestoreExecutionPlan(next, plan) != nil ||
		read.Values[3].ModRevision != next.CurrentTarget.Volume.HeadRevision ||
		current.Record.ID != next.TaskID || current.Record.OperationID != next.OperationID ||
		current.Record.Owner.EnvironmentID != next.EnvironmentID || !current.Record.CreatedAt.Equal(next.CreatedAt) ||
		current.Record.PlanID != plan.PlanId || current.Record.PlanHash != hex.EncodeToString(plan.PlanHash) ||
		read.Values[1].ModRevision > restored.Revision {
		return TaskRecord{}, nil, nil, errs.New(errs.KindStateConflict, "Volume Restore terminal authority changed")
	}
	headSHA := sha256.Sum256(read.Values[3].Value)
	if hex.EncodeToString(headSHA[:]) != next.CurrentTarget.Volume.HeadSHA256 {
		return TaskRecord{}, nil, nil, errs.New(errs.KindStateConflict, "Volume Restore predecessor changed")
	}
	fence, err := environmentfence.LoadOwned(ctx, repository.store, next.EnvironmentID, restored.ReadRevision,
		environmentfence.Owner{Kind: backupruntime.BackupOperationRestore,
			OperationID: next.OperationID, TaskID: next.TaskID})
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	value, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer clear(value)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer clear(epoch.Value)
	domainConditions := make([]etcdstore.Condition, 0, len(keys))
	for index, key := range keys {
		domainConditions = append(
			domainConditions,
			etcdstore.Condition{Key: key, ModRevision: read.Values[index].ModRevision},
		)
	}
	domainConditions, err = environmentfence.AppendConditions(domainConditions, fence)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	domainMutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[0], Value: value}, epoch}
	if next.State != backupruntime.BackupRestoreRecoveryRequired {
		domainMutations = append(domainMutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: hierarchy.EnvironmentOperationLockKey(next.EnvironmentID)})
	}
	evidence, err := backupTerminalTaskEvidence(taskPlan.record)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	digest, err := backupruntime.BackupRestoreTerminalDomainDigest(next)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	receipt, err := prepareBackupTerminalReceiptPlan(backupruntime.BackupTerminalReceiptRecord{
		Task: evidence, PriorTaskRevision: current.Revision, DomainDigest: digest, Restore: &next})
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer receipt.clear()
	conditions, mutations, err := composeBackupTerminalTransaction(taskPlan.conditions, taskPlan.mutations,
		domainConditions, domainMutations, receipt)
	return taskPlan.record, conditions, mutations, err
}
