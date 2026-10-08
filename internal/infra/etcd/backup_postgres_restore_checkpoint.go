package etcd

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/databaserestoreauthority"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) CheckpointDatabaseRestore(ctx context.Context,
	input backupruntime.BackupCheckpointInput,
	current etcdstore.Versioned[backupruntime.BackupRestoreRecord], at time.Time,
) (int64, error) {
	if backupruntime.ValidateBackupCheckpointInput(input) != nil || current.Revision <= 0 ||
		current.Record.TaskID != input.TaskID ||
		current.Record.Point.SourceKind != backupruntime.BackupRuntimeSourceAttach {
		return 0, errs.New(errs.KindStateConflict, "database Restore checkpoint authority is invalid")
	}
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(current.Record.EnvironmentID, input.TaskID)
	if err != nil {
		return 0, err
	}
	keys := []string{backupruntime.BackupRestoreKey(input.TaskID), membership,
		backupruntime.BackupExecutionPlanKey(input.TaskID), taskjournal.TaskStorageKey(input.TaskID)}
	read, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return 0, errs.New(errs.KindStateConflict, "database Restore checkpoint authority is incomplete")
		}
	}
	stored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || read.Values[0].ModRevision != current.Revision ||
		!backupruntime.BackupRestoreRecordsEqual(stored, current.Record) || read.Values[1].Version != 1 ||
		string(read.Values[1].Value) != input.TaskID || read.Values[1].ModRevision > current.Revision {
		return 0, errs.New(errs.KindStateConflict, "database Restore native cursor changed")
	}
	plan, err := backupruntime.DecodeBackupExecutionPlan(read.Values[2].Value)
	if err != nil || read.Values[2].Version != 1 ||
		backupruntime.ValidateDatabaseRestoreExecutionPlan(stored, plan) != nil {
		return 0, errs.New(errs.KindStateConflict, "database Restore sealed plan changed")
	}
	task, err := DecodeTaskRecord(read.Values[3].Value)
	if err != nil || task.ID != input.TaskID || task.OperationID != stored.OperationID ||
		task.Type != taskjournal.TaskRestore || task.Status != taskjournal.TaskStatusRunning ||
		task.PlanID != plan.PlanId || task.PlanHash != hex.EncodeToString(plan.PlanHash) ||
		plan.Steps[0].StepId != input.StepID ||
		plan.Steps[0].GetBackupStep().ExecutionId != input.ExecutionID ||
		hex.EncodeToString(plan.Steps[0].GetBackupStep().StepDigest) != input.AuthoritySHA256 {
		return 0, errs.New(errs.KindStateConflict, "database Restore Task checkpoint binding changed")
	}
	authority, err := databaserestoreauthority.Read(ctx, repository.store, stored, read.ReadRevision)
	if err != nil {
		return 0, err
	}
	next, err := backupruntime.PrepareDatabaseRestoreCheckpoint(stored, input.Request, at)
	if err != nil {
		return 0, err
	}
	fence, err := environmentfence.LoadOwned(ctx, repository.store, stored.EnvironmentID,
		read.ReadRevision, environmentfence.Owner{Kind: backupruntime.BackupOperationRestore,
			OperationID: stored.OperationID, TaskID: input.TaskID})
	if err != nil {
		return 0, err
	}
	backingEnvironmentID, found, err := backupruntime.DatabaseRestoreBackingEnvironmentID(stored)
	if err != nil || !found {
		return 0, errs.New(errs.KindStateConflict, "database Restore backing guard authority is invalid")
	}
	backingGuard, err := postgresbackingguard.PrepareOwnership(
		ctx,
		repository.store,
		[]string{backingEnvironmentID},
		stored.EnvironmentID,
		postgresbackingguard.Owner(backupruntime.BackupOperationRestore, stored.OperationID, stored.TaskID),
		read.ReadRevision,
		false,
	)
	if err != nil {
		return 0, err
	}
	defer backingGuard.Clear()
	conditions := make([]etcdstore.Condition, 0, len(keys)+len(authority)+4+len(backingGuard.Conditions))
	for index, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	conditions = append(conditions, authority...)
	conditions, err = environmentfence.AppendConditions(conditions, fence)
	if err != nil {
		return 0, err
	}
	conditions = append(conditions, backingGuard.Conditions...)
	checkpoint, err := repository.loadBackupCheckpointPlan(ctx, input, read.ReadRevision,
		backupCheckpointBinding{taskType: taskjournal.TaskRestore, pointID: stored.Point.ID})
	if err != nil {
		return 0, err
	}
	defer checkpoint.clear()
	if checkpoint.duplicate {
		return checkpoint.commitRevision, nil
	}
	value, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		return 0, err
	}
	defer clear(value)
	conditions, mutations, err := checkpoint.composeTransaction(conditions,
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[0], Value: value}})
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	result, err := repository.TransactRuntime(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "database Restore checkpoint authority changed")
	}
	return result.Revision, nil
}
