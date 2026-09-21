package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) retryBackupKeyRotationTask(
	ctx context.Context,
	source etcdstore.Versioned[TaskRecord],
	retryTaskID string,
	actor taskjournal.TaskActor,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if actor != taskjournal.TaskActorOperator {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation retry actor must be operator",
		)
	}
	retry, err := CloneRetryTask(source.Record, retryTaskID, actor, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != retry.ID ||
		!marker.CreatedAt.Equal(retry.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation retry marker does not match its Task",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		backupruntime.BackupKeyRotationKey(source.Record.ID),
		backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, source.Record.ID),
		backupruntime.BackupKeyRotationKey(retry.ID),
		backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, retry.ID),
		backuppolicy.BackupKeyKey(
			source.Record.Owner.EnvironmentID,
		), backuppolicy.BackupKeyValueKey(source.Record.Owner.EnvironmentID),
		hierarchyrecord.EnvironmentOperationLockKey(source.Record.Owner.EnvironmentID),
	}, Revision: source.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if read == nil || read.ReadRevision != source.ReadRevision || len(read.Values) != 7 || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[2] != nil ||
		read.Values[3] != nil ||
		read.Values[6] != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation retry authority changed",
		)
	}
	defer etcdstore.ClearValues(read.Values)
	rotation, err := backupruntime.DecodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil || rotation.TaskID != source.Record.ID || rotation.State != backupruntime.BackupKeyRotationPrepared {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable,
			"backup key rotation authority is not retryable",
		)
	}
	current, err := backuppolicy.DecodeBackupKeyRecord(read.Values[4].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, backuppolicy.CorruptBackupKey()
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[5].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, backuppolicy.CorruptBackupKey()
	}
	defer clear(currentValue.Ciphertext)
	if current.KeyEra != rotation.CurrentKeyEra ||
		read.Values[4].ModRevision != rotation.ExpectedCurrentRecordRevision ||
		read.Values[5].ModRevision != rotation.ExpectedCurrentValueRevision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backup key changed before rotation retry",
		)
	}
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		source.Record.Owner.EnvironmentID,
		source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	newIndex := backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, retry.ID)
	rotation.TaskID = retry.ID
	rotation.UpdatedAt = retry.CreatedAt
	rotationValue, err := backupruntime.EncodeBackupKeyRotationRecord(rotation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(backupruntime.BackupOperationLockRecord{
		EnvironmentID: source.Record.Owner.EnvironmentID, OperationID: retry.OperationID, TaskID: retry.ID,
		Kind: backupruntime.BackupOperationRotation, CreatedAt: rotation.CreatedAt, UpdatedAt: retry.CreatedAt,
	})
	if err != nil {
		clear(rotationValue)
		return IdempotencyTransactionResult{}, err
	}
	taskValue, err := EncodeTaskRecord(retry)
	if err != nil {
		clear(rotationValue)
		clear(lockValue)
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(retry.ID)
	if err != nil {
		clear(rotationValue)
		clear(lockValue)
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []etcdstore.Condition{
		{
			Key: taskjournal.TaskStorageKey(retry.ID),
		}, {Key: taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID)},
		{
			Key: taskjournal.TaskActiveOperationKey(retry.OperationID),
		}, {Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID)},
		{Key: backupruntime.BackupKeyRotationKey(source.Record.ID), ModRevision: read.Values[0].ModRevision},
		{
			Key: backupKeyRotationEnvironmentIndexKeyForRetry(
				source.Record.Owner.EnvironmentID,
				source.Record.ID,
			),
			ModRevision: read.Values[1].ModRevision,
		},
		{Key: backupruntime.BackupKeyRotationKey(retry.ID)}, {Key: newIndex},
		{Key: backuppolicy.BackupKeyKey(source.Record.Owner.EnvironmentID), ModRevision: read.Values[4].ModRevision},
		{
			Key:         backuppolicy.BackupKeyValueKey(source.Record.Owner.EnvironmentID),
			ModRevision: read.Values[5].ModRevision,
		},
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(retry.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID), Value: reference},
		{Type: etcdstore.MutationDelete, Key: backupruntime.BackupKeyRotationKey(source.Record.ID)},
		{
			Type: etcdstore.MutationDelete,
			Key:  backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, source.Record.ID),
		},
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupKeyRotationKey(retry.ID), Value: rotationValue},
		{Type: etcdstore.MutationPut, Key: newIndex, Value: []byte(retry.ID)},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentOperationLockKey(source.Record.Owner.EnvironmentID),
			Value: lockValue,
		},
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	mutations = append(mutations, epoch)
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "backup key rotation retry evidence is incomplete")
		}
		return errs.New(errs.KindStateConflict, "backup key rotation retry state changed")
	}
	initiation, err := newInheritedTaskInitiation(source, actor)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(retry, initiation, conditions, mutations, classify)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	result, err := (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
	clear(rotationValue)
	clear(lockValue)
	return result, err
}

func backupKeyRotationEnvironmentIndexKeyForRetry(environmentID, taskID string) string {
	key, _ := backupruntime.BackupKeyRotationEnvironmentIndexKey(environmentID, taskID)
	return key
}
