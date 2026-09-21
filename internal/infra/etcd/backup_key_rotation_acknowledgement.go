package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// ApplyBackupKeyRotation swaps the current public key and wrapped identity.
// The operation lock remains part of the same transaction and is released
// only after the applied rotation authority is durable.
func (repository *BackupPolicyRepository) ApplyBackupKeyRotation(ctx context.Context, taskID string) error {
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "backup policy repository is not configured")
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return err
	}
	task, err := tasks.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task.Record.Type != taskjournal.TaskRotate || task.Record.Executor != taskjournal.TaskExecutorController ||
		task.Record.Status != taskjournal.TaskStatusRunning {
		return errs.New(errs.KindStateConflict, "backup key rotation Task is not running")
	}
	change, err := tasks.prepareBackupKeyRotationTaskAcknowledgement(
		ctx, task.Record, taskjournal.TaskStatusCompleted, task.Record.UpdatedAt, task.ReadRevision,
	)
	change.clear()
	return err
}

type backupKeyRotationTaskChange struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (change *backupKeyRotationTaskChange) clear() {
	if change == nil {
		return
	}
	clearMutationValues(change.mutations)
	change.conditions = nil
	change.mutations = nil
}

func (repository *TaskRepository) prepareBackupKeyRotationTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	terminalAt time.Time,
	readRevision int64,
) (backupKeyRotationTaskChange, error) {
	if task.Type != taskjournal.TaskRotate {
		return backupKeyRotationTaskChange{}, nil
	}
	if task.Executor != taskjournal.TaskExecutorController || task.Target != task.Owner.EnvironmentID ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || !isTerminalTaskStatus(status) ||
		!backuppolicy.ValidUTCInstant(terminalAt) || readRevision <= 0 {
		return backupKeyRotationTaskChange{}, errs.New(
			errs.KindInternal,
			"backup key rotation Task acknowledgement is invalid",
		)
	}
	keys := []string{
		backupruntime.BackupKeyRotationKey(task.ID),
		backuppolicy.BackupKeyKey(task.Target),
		backuppolicy.BackupKeyValueKey(task.Target),
		hierarchyrecord.EnvironmentOperationLockKey(task.Target),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return backupKeyRotationTaskChange{}, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil {
		return backupKeyRotationTaskChange{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation acknowledgement authority changed",
		)
	}
	defer clearKeyValues(read.Values)
	rotation, err := backupruntime.DecodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil {
		return backupKeyRotationTaskChange{}, corruptBackupKey()
	}
	defer clear(rotation.NextEncryptedIdentity)
	current, err := backuppolicy.DecodeBackupKeyRecord(read.Values[1].Value)
	if err != nil {
		return backupKeyRotationTaskChange{}, corruptBackupKey()
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[2].Value)
	if err != nil {
		return backupKeyRotationTaskChange{}, corruptBackupKey()
	}
	defer clear(currentValue.Ciphertext)
	lock, err := backupruntime.DecodeBackupOperationLockRecord(read.Values[3].Value)
	if err != nil || rotation.State != backupruntime.BackupKeyRotationPrepared ||
		rotation.TaskID != task.ID || rotation.OperationID != task.OperationID ||
		rotation.EnvironmentID != task.Target || current.EnvironmentID != task.Target ||
		currentValue.EnvironmentID != task.Target || current.KeyEra != rotation.CurrentKeyEra ||
		currentValue.KeyEra != rotation.CurrentKeyEra ||
		read.Values[1].ModRevision != rotation.ExpectedCurrentRecordRevision ||
		read.Values[2].ModRevision != rotation.ExpectedCurrentValueRevision ||
		lock.TaskID != task.ID || lock.OperationID != task.OperationID ||
		lock.Kind != backupruntime.BackupOperationRotation {
		return backupKeyRotationTaskChange{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation acknowledgement authority changed",
		)
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		task.Target,
		readRevision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationRotation, OperationID: task.OperationID, TaskID: task.ID,
		},
	)
	if err != nil {
		return backupKeyRotationTaskChange{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupKeyRotationKey(task.ID), ModRevision: read.Values[0].ModRevision},
		{Key: backuppolicy.BackupKeyKey(task.Target), ModRevision: read.Values[1].ModRevision},
		{Key: backuppolicy.BackupKeyValueKey(task.Target), ModRevision: read.Values[2].ModRevision},
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := make([]etcdstore.Mutation, 0, 5)
	if status == taskjournal.TaskStatusCompleted {
		nextRecord := backuppolicy.BackupKeyRecord{
			EnvironmentID: task.Target,
			Recipient:     rotation.NextRecipient,
			KeyEra:        rotation.NextKeyEra,
			CreatedAt:     current.CreatedAt,
			RotatedAt:     terminalAt.UTC(),
		}
		recordValue, encodeErr := backuppolicy.EncodeBackupKeyRecord(nextRecord)
		if encodeErr != nil {
			return backupKeyRotationTaskChange{}, encodeErr
		}
		nextValue := backuppolicy.BackupKeyEncryptedValue{
			EnvironmentID: task.Target,
			KeyEra:        rotation.NextKeyEra,
			Ciphertext:    append([]byte(nil), rotation.NextEncryptedIdentity...),
		}
		valueValue, encodeErr := backuppolicy.EncodeBackupKeyEncryptedValue(nextValue)
		clear(nextValue.Ciphertext)
		if encodeErr != nil {
			clear(recordValue)
			return backupKeyRotationTaskChange{}, encodeErr
		}
		rotation.State = backupruntime.BackupKeyRotationApplied
		rotation.UpdatedAt = terminalAt.UTC()
		clear(rotation.NextEncryptedIdentity)
		rotation.NextEncryptedIdentity = nil
		rotationValue, encodeErr := backupruntime.EncodeBackupKeyRotationRecord(rotation)
		if encodeErr != nil {
			clear(recordValue)
			clear(valueValue)
			return backupKeyRotationTaskChange{}, encodeErr
		}
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backuppolicy.BackupKeyKey(task.Target), Value: recordValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backuppolicy.BackupKeyValueKey(task.Target), Value: valueValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backupruntime.BackupKeyRotationKey(task.ID), Value: rotationValue},
		)
	}
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(task.Target)},
	)
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		clearMutationValues(mutations)
		return backupKeyRotationTaskChange{}, err
	}
	mutations = append(mutations, epoch)
	return backupKeyRotationTaskChange{conditions: conditions, mutations: mutations}, nil
}
