package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareEnvironmentRemovalTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (environmentTaskChange, error) {
	if source.Executor != taskjournal.TaskExecutorAgent || source.Type != taskjournal.TaskRemove ||
		recordcodec.ValidateID(ids.KindEnvironment, source.Target) != nil {
		return environmentTaskChange{}, nil
	}
	if retry.Executor != source.Executor || retry.Type != source.Type ||
		retry.Target != source.Target ||
		retry.OperationID != source.OperationID ||
		retry.RetryOf != source.ID {
		return environmentTaskChange{}, errs.New(
			errs.KindInternal,
			"environment deletion retry changed its durable target",
		)
	}
	if source.RetainUntil == nil {
		return environmentTaskChange{}, errs.New(
			errs.KindInternal,
			"environment deletion retry source retention is missing",
		)
	}
	retentionKey := taskjournal.TaskRetentionIndexKey(source.ID, *source.RetainUntil)
	owner := environmentfence.Owner{
		Kind: backupruntime.BackupOperationDeletion, OperationID: source.OperationID, TaskID: source.ID,
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		source.Target,
		revision,
		owner,
	)
	if err != nil {
		return environmentTaskChange{}, err
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), source.Target),
			hierarchyrecord.EnvironmentOperationLockKey(source.Target),
			retentionKey,
		},
		Revision: revision,
	})
	if err != nil {
		return environmentTaskChange{}, err
	}
	if state == nil {
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"environment deletion retry ownership is missing",
		)
	}
	if state.ReadRevision != revision || len(state.Values) != 3 || state.Values[0] == nil ||
		state.Values[1] == nil {
		etcdstore.ClearValues(state.Values)
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"environment deletion retry ownership is missing",
		)
	}
	defer etcdstore.ClearValues(state.Values)
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != source.Target ||
		tombstone.TaskID != source.ID {
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"environment deletion retry tombstone ownership changed",
		)
	}
	lock, err := decodeOwnedEnvironmentDeletionLock(state.Values[1], source)
	if err != nil {
		return environmentTaskChange{}, err
	}
	retentionCondition := etcdstore.Condition{Key: retentionKey}
	if state.Values[2] != nil {
		retainedTaskID, decodeErr := idempotencyrecord.DecodeTaskReference(state.Values[2].Value)
		if decodeErr != nil || retainedTaskID != source.ID {
			return environmentTaskChange{}, errs.New(
				errs.KindInternal,
				"environment deletion retry source retention is corrupt",
			)
		}
		retentionCondition.ModRevision = state.Values[2].ModRevision
	}
	intent, err := loadEnvironmentDeletionIntent(ctx, repository.store, source, tombstone, revision)
	if err != nil {
		return environmentTaskChange{}, err
	}
	tombstone.TaskID = retry.ID
	tombstone.UpdatedAt = retry.CreatedAt
	lock.TaskID = retry.ID
	lock.UpdatedAt = retry.CreatedAt
	intent.Record.TaskID = retry.ID
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return environmentTaskChange{}, err
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		clear(tombstoneValue)
		return environmentTaskChange{}, err
	}
	intentValue, err := encodeEnvironmentDeletionIntent(intent.Record)
	if err != nil {
		clear(tombstoneValue)
		clear(lockValue)
		return environmentTaskChange{}, err
	}
	retentionValue, err := idempotencyrecord.EncodeTaskReference(source.ID)
	if err != nil {
		clear(tombstoneValue)
		clear(lockValue)
		clear(intentValue)
		return environmentTaskChange{}, err
	}
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		clear(tombstoneValue)
		clear(lockValue)
		clear(intentValue)
		clear(retentionValue)
		return environmentTaskChange{}, err
	}
	conditions := fence.TransactionConditions()
	conditions = append(conditions,
		etcdstore.Condition{
			Key: environmentDeletionIntentKey(source.OperationID), ModRevision: intent.Revision,
		},
		retentionCondition,
	)
	return environmentTaskChange{
		applies:    true,
		conditions: conditions,
		mutations: []etcdstore.Mutation{
			{
				Type:  etcdstore.MutationPut,
				Key:   deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), source.Target),
				Value: tombstoneValue,
			},
			{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentOperationLockKey(source.Target), Value: lockValue},
			{
				Type:  etcdstore.MutationPut,
				Key:   environmentDeletionIntentKey(source.OperationID),
				Value: intentValue,
			},
			epochMutation,
			{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
		},
		values: [][]byte{
			tombstoneValue, lockValue, intentValue, epochMutation.Value, retentionValue,
		},
	}, nil
}
