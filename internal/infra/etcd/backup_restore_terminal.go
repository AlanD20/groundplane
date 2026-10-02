package etcd

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareConfigRestoreSuccessfulTerminal(ctx context.Context,
	runtime *BackupRuntimeRepository, current TaskAssignment, result taskjournal.TaskResultRecord, at time.Time,
) (TaskRecord, []etcdstore.Condition, []etcdstore.Mutation, error) {
	restored, err := runtime.GetBackupRestore(ctx, current.Task.Record.ID)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	if restored.Record.Point.SourceKind == backupruntime.BackupRuntimeSourceVolume {
		return repository.prepareVolumeRestoreSuccessfulTerminal(ctx, runtime, current, result, at, restored)
	}
	if restored.Record.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
		return repository.preparePostgresRestoreSuccessfulTerminal(ctx, runtime, current, result, at, restored)
	}
	at = backupTerminalTimestamp(at, restored.Record.UpdatedAt)
	taskPlan, err := repository.prepareBackupTaskTerminal(ctx, current, taskjournal.TaskStatusCompleted, result, at)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer taskPlan.clear()
	next, err := backupruntime.CompleteConfigRestore(restored.Record, *taskPlan.record.FinishedAt)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	return repository.composeConfigRestoreTerminal(ctx, runtime, current.Task, restored, next, taskPlan)
}

func (repository *TaskRepository) preparePendingConfigRestoreTerminal(
	ctx context.Context,
	runtime *BackupRuntimeRepository,
	current etcdstore.Versioned[TaskRecord],
	status taskjournal.TaskStatus,
	at time.Time,
) (TaskRecord, []etcdstore.Condition, []etcdstore.Mutation, error) {
	restored, err := runtime.GetBackupRestore(ctx, current.Record.ID)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	if restored.Record.Point.SourceKind == backupruntime.BackupRuntimeSourceVolume {
		return repository.preparePendingVolumeRestoreTerminal(ctx, runtime, current, status, at, restored)
	}
	if restored.Record.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
		return repository.preparePendingPostgresRestoreTerminal(ctx, runtime, current, status, at, restored)
	}
	if restored.Record.State != backupruntime.BackupRestoreQueued {
		return TaskRecord{}, nil, nil, errs.New(errs.KindStateConflict, "pending Restore has execution progress")
	}
	at = backupTerminalTimestamp(at, restored.Record.UpdatedAt)
	taskPlan, err := repository.preparePendingBackupTaskTerminal(ctx, current, status, at)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	defer taskPlan.clear()
	next, err := backupruntime.FailConfigRestoreBeforeMutation(restored.Record, *taskPlan.record.FinishedAt)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	return repository.composeConfigRestoreTerminal(ctx, runtime, current, restored, next, taskPlan)
}

func (repository *TaskRepository) composeConfigRestoreTerminal(ctx context.Context,
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
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return TaskRecord{}, nil, nil, errs.New(errs.KindStateConflict, "Restore terminal authority is incomplete")
		}
	}
	plan, err := backupruntime.DecodeBackupExecutionPlan(read.Values[2].Value)
	headID, headErr := idempotency.DecodeTaskReference(read.Values[3].Value)
	expectedHead, expectedHeadRevision := next.CurrentTarget.Config.BaselineRevisionID, next.CurrentTarget.Config.BaselineHeadRevision
	if next.State == backupruntime.BackupRestoreCompleted || next.ConfigProgress.PublishedHeadRevision > 0 {
		expectedHead, expectedHeadRevision = next.TaskID, next.ConfigProgress.PublishedHeadRevision
	}
	if next.State == backupruntime.BackupRestoreRecoveryRequired && headErr == nil && headID == next.TaskID &&
		next.ConfigProgress.PublishedHeadRevision == 0 &&
		next.ConfigProgress.DeleteEntryOrdinal == next.ConfigProgress.DeleteEntryCount &&
		next.ConfigProgress.UpsertEntryOrdinal == next.Point.ConfigArchive.EntryCount &&
		read.Values[3].ModRevision == restored.Revision && read.Values[3].ModRevision > next.ConfigProgress.RevisionRootRevision {
		// The head switch and native record were atomic, but the independent
		// head-revision receipt may have been interrupted. Retain that exact
		// evidence rather than substituting a guessed revision or completion.
		expectedHead, expectedHeadRevision = next.TaskID, read.Values[3].ModRevision
	}
	if err != nil || headErr != nil || read.Values[0].ModRevision != restored.Revision ||
		read.Values[1].Version != 1 || string(read.Values[1].Value) != next.TaskID ||
		read.Values[2].Version != 1 || backupruntime.ValidateConfigRestoreExecutionPlan(next, plan) != nil ||
		headID != expectedHead || read.Values[3].ModRevision != expectedHeadRevision ||
		current.Record.ID != next.TaskID || current.Record.OperationID != next.OperationID ||
		current.Record.Owner.EnvironmentID != next.EnvironmentID || !current.Record.CreatedAt.Equal(next.CreatedAt) ||
		current.Record.PlanID != plan.PlanId || current.Record.PlanHash != hex.EncodeToString(plan.PlanHash) ||
		read.Values[1].ModRevision > restored.Revision {
		return TaskRecord{}, nil, nil, errs.New(errs.KindStateConflict, "Restore terminal authority changed")
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		next.EnvironmentID,
		restored.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationRestore,
			OperationID: next.OperationID,
			TaskID:      next.TaskID,
		},
	)
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
		domainMutations = append(
			domainMutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  hierarchy.EnvironmentOperationLockKey(next.EnvironmentID),
			},
		)
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
