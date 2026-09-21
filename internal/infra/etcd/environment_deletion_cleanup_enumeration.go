package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
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
) (etcdstore.Versioned[EnvironmentDeletionIntentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		recordcodec.ValidateID(ids.KindEnvironment, task.Target) != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"environment deletion cleanup task is invalid",
		)
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
		taskjournal.TaskStorageKey(task.ID),
	}})
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if state == nil || state.ReadRevision <= 0 || len(state.Values) != 2 ||
		state.Values[0] == nil ||
		state.Values[1] == nil {
		if state != nil {
			etcdstore.ClearValues(state.Values)
		}
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup ownership is missing",
		)
	}
	defer etcdstore.ClearValues(state.Values)
	persistedTask, err := decodeTaskRecord(state.Values[1].Value)
	if err != nil || persistedTask.ID != task.ID || persistedTask.OperationID != task.OperationID ||
		persistedTask.Executor != task.Executor || persistedTask.Type != task.Type ||
		persistedTask.Target != task.Target || persistedTask.Status != task.Status {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup task ownership changed",
		)
	}
	if taskjournal.IsTerminalTaskStatus(persistedTask.Status) {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"terminal environment deletion task cannot complete cleanup enumeration",
		)
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != task.Target || tombstone.TaskID != task.ID {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup tombstone ownership changed",
		)
	}
	owner := environmentMutationFenceOwner{
		Kind: backupruntime.BackupOperationDeletion, OperationID: task.OperationID, TaskID: task.ID,
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx, repository.store, task.Target, state.ReadRevision, owner,
	)
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	intent, err := loadEnvironmentDeletionIntent(
		ctx, repository.store, task, tombstone, state.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if intent.Record.CleanupPhase == EnvironmentDeletionCleanupComplete {
		return intent, nil
	}
	if err := requireEnvironmentDeletionBackupStateEmpty(
		ctx, repository.store, task.Target, task.OperationID, state.ReadRevision,
	); err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	next := intent.Record
	next.CleanupPhase = EnvironmentDeletionCleanupComplete
	intentValue, err := encodeEnvironmentDeletionIntent(next)
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	defer clear(intentValue)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := fence.transactionConditions()
	conditions = append(conditions,
		etcdstore.Condition{
			Key: environmentDeletionIntentKey(task.OperationID), ModRevision: intent.Revision,
		},
		etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: state.Values[1].ModRevision},
	)
	transaction, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   environmentDeletionIntentKey(task.OperationID),
			Value: intentValue,
		},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup completion changed",
		)
	}
	return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}
