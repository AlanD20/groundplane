package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateBackupKeyRotationTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	readRevision int64,
) error {
	if task.Type != taskjournal.TaskRotate {
		return nil
	}
	keys := []string{
		backupruntime.BackupKeyRotationKey(task.ID),
		backuppolicy.BackupKeyKey(task.Target),
		backuppolicy.BackupKeyValueKey(task.Target),
		hierarchyrecord.EnvironmentOperationLockKey(task.Target),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] != nil {
		return errs.New(errs.KindStateConflict, "backup key rotation terminal replay changed")
	}
	defer etcdstore.ClearValues(read.Values)
	rotation, err := backupruntime.DecodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil {
		return backuppolicy.CorruptBackupKey()
	}
	defer clear(rotation.NextEncryptedIdentity)
	current, err := backuppolicy.DecodeBackupKeyRecord(read.Values[1].Value)
	if err != nil {
		return backuppolicy.CorruptBackupKey()
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[2].Value)
	if err != nil {
		return backuppolicy.CorruptBackupKey()
	}
	defer clear(currentValue.Ciphertext)
	if rotation.TaskID != task.ID || rotation.OperationID != task.OperationID ||
		rotation.EnvironmentID != task.Target || current.EnvironmentID != task.Target ||
		currentValue.EnvironmentID != task.Target || current.KeyEra != currentValue.KeyEra {
		return errs.New(errs.KindStateConflict, "backup key rotation terminal replay changed")
	}
	if status == taskjournal.TaskStatusCompleted {
		if rotation.State != backupruntime.BackupKeyRotationApplied || len(rotation.NextEncryptedIdentity) != 0 ||
			current.KeyEra != rotation.NextKeyEra || current.Recipient != rotation.NextRecipient {
			return errs.New(errs.KindStateConflict, "backup key rotation completion replay changed")
		}
		return nil
	}
	if rotation.State != backupruntime.BackupKeyRotationPrepared || current.KeyEra != rotation.CurrentKeyEra ||
		read.Values[1].ModRevision != rotation.ExpectedCurrentRecordRevision ||
		read.Values[2].ModRevision != rotation.ExpectedCurrentValueRevision {
		return errs.New(errs.KindStateConflict, "backup key rotation failed-attempt replay changed")
	}
	return nil
}
