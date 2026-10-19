package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The key swap, lock release and work result commit together. A restart cannot
// observe a changed key with unfinished execution or repeat an acknowledged swap.
func (repository *BackupPolicyRepository) PrepareBackupKeyRotationSettlement(ctx context.Context,
	view workqueue.View, outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.ReadRevision <= 0 || task.Action != workexecution.ActionRotate ||
		item.Operation != workexecution.SecretRotation ||
		item.TaskID != task.ID ||
		item.TargetID != task.TargetID ||
		item.Input == nil ||
		item.Placement.Executor != workexecution.Controller ||
		item.Placement.HostID != "" ||
		item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone ||
		(item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting) ||
		at.Before(item.UpdatedAt) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation is not ready for atomic settlement",
		)
	}
	if outcome == workexecution.Succeeded &&
		(task.Stop != nil || item.ForwardRevoked || !at.Before(task.Deadline) || !at.Before(item.Deadline)) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation permission was withdrawn or expired",
		)
	}
	keys := []string{
		backupruntime.BackupKeyRotationKey(task.ID),
		backuppolicy.BackupKeyKey(task.TargetID),
		backuppolicy.BackupKeyValueKey(task.TargetID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation input is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return workqueue.Change{}, backuppolicy.CorruptBackupKey()
		}
	}
	rotation, err := backupruntime.DecodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(rotation.NextEncryptedIdentity)
	reference, err := backupruntime.KeyRotationInput(rotation)
	if err != nil {
		return workqueue.Change{}, err
	}
	if rotation.State != backupruntime.BackupKeyRotationPrepared || rotation.TaskID != task.ID ||
		rotation.WorkItemID != item.ID || rotation.EnvironmentID != task.TargetID || task.Scope.EnvironmentID != task.TargetID ||
		reference != task.Intent || reference != *item.Input {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation differs from captured input")
	}
	current, err := backuppolicy.DecodeBackupKeyRecord(read.Values[1].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[2].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(currentValue.Ciphertext)
	if current.EnvironmentID != rotation.EnvironmentID || currentValue.EnvironmentID != rotation.EnvironmentID ||
		current.KeyEra != rotation.CurrentKeyEra || currentValue.KeyEra != rotation.CurrentKeyEra ||
		read.Values[1].ModRevision != rotation.ExpectedCurrentRecordRevision || read.Values[2].ModRevision != rotation.ExpectedCurrentValueRevision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation current key changed")
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		rotation.EnvironmentID,
		view.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationRotation,
			OperationID: rotation.OperationID,
			TaskID:      task.ID,
		},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: fence.TransactionConditions()}
	for index, key := range keys {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision},
		)
	}
	if outcome == workexecution.Succeeded {
		key := backuppolicy.BackupKeyRecord{EnvironmentID: rotation.EnvironmentID, Recipient: rotation.NextRecipient,
			KeyEra: rotation.NextKeyEra, CreatedAt: current.CreatedAt, RotatedAt: at.UTC()}
		raw, err := backuppolicy.EncodeBackupKeyRecord(key)
		if err != nil {
			return workqueue.Change{}, err
		}
		value, err := backuppolicy.EncodeBackupKeyEncryptedValue(backuppolicy.BackupKeyEncryptedValue{
			EnvironmentID: rotation.EnvironmentID, KeyEra: rotation.NextKeyEra, Ciphertext: rotation.NextEncryptedIdentity})
		if err != nil {
			clear(raw)
			return workqueue.Change{}, err
		}
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[1], Value: raw},
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[2], Value: value},
		)
		rotation.State = backupruntime.BackupKeyRotationApplied
	} else {
		rotation.State = backupruntime.BackupKeyRotationDiscarded
	}
	clear(rotation.NextEncryptedIdentity)
	rotation.NextEncryptedIdentity, rotation.UpdatedAt = nil, at.UTC()
	raw, err := backupruntime.EncodeBackupKeyRotationRecord(rotation)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		clear(raw)
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[0], Value: raw},
		keyvalue.Mutation{
			Type: keyvalue.MutationDelete,
			Key:  hierarchy.EnvironmentOperationLockKey(rotation.EnvironmentID),
		},
		epoch,
	)
	settled, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item,
		workexecution.Completion{Outcome: outcome, Evidence: reference, EffectsSettled: true}, at)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, settled.Conditions...)
	change.Mutations = append(change.Mutations, settled.Mutations...)
	return change, nil
}
