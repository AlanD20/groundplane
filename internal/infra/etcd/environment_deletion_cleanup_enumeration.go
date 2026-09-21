package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CompleteEnvironmentDeletionCleanupEnumeration records affirmative completion
// only while the exact deletion owner is retained and every Environment-
// addressable Backup authority is absent at one fixed revision.
func (repository *TaskRepository) CompleteEnvironmentDeletionCleanupEnumeration(
	ctx context.Context,
	task TaskRecord,
) (etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		recordcodec.ValidateID(ids.KindEnvironment, task.Target) != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"environment deletion cleanup task is invalid",
		)
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
		taskjournal.TaskStorageKey(task.ID),
	}})
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	if state == nil || state.ReadRevision <= 0 || len(state.Values) != 2 ||
		state.Values[0] == nil ||
		state.Values[1] == nil {
		if state != nil {
			etcdstore.ClearValues(state.Values)
		}
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup ownership is missing",
		)
	}
	defer etcdstore.ClearValues(state.Values)
	persistedTask, err := DecodeTaskRecord(state.Values[1].Value)
	if err != nil || persistedTask.ID != task.ID || persistedTask.OperationID != task.OperationID ||
		persistedTask.Executor != task.Executor || persistedTask.Type != task.Type ||
		persistedTask.Target != task.Target || persistedTask.Status != task.Status {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup task ownership changed",
		)
	}
	if taskjournal.IsTerminalTaskStatus(persistedTask.Status) {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"terminal environment deletion task cannot complete cleanup enumeration",
		)
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != task.Target || tombstone.TaskID != task.ID {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup tombstone ownership changed",
		)
	}
	owner := environmentfence.Owner{
		Kind: backupruntime.BackupOperationDeletion, OperationID: task.OperationID, TaskID: task.ID,
	}
	fence, err := environmentfence.LoadOwned(
		ctx, repository.store, task.Target, state.ReadRevision, owner,
	)
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	intent, err := loadEnvironmentDeletionIntent(
		ctx, repository.store, task, tombstone, state.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	if intent.Record.CleanupPhase == deletionrecord.EnvironmentDeletionCleanupComplete {
		return intent, nil
	}
	if err := requireEnvironmentDeletionBackupStateEmpty(
		ctx, repository.store, task.Target, task.OperationID, state.ReadRevision,
	); err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	next := intent.Record
	next.CleanupPhase = deletionrecord.EnvironmentDeletionCleanupComplete
	intentValue, err := deletionrecord.EncodeEnvironmentDeletionIntent(next)
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	defer clear(intentValue)
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := fence.TransactionConditions()
	conditions = append(conditions,
		etcdstore.Condition{
			Key: deletionrecord.EnvironmentDeletionIntentKey(task.OperationID), ModRevision: intent.Revision,
		},
		etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: state.Values[1].ModRevision},
	)
	transaction, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   deletionrecord.EnvironmentDeletionIntentKey(task.OperationID),
			Value: intentValue,
		},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup completion changed",
		)
	}
	return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}
