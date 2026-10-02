package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetBackupRestore requires the original Environment membership at the same
// view as its native record. A Task or desired head cannot substitute for it.
func (reader *Reader) GetBackupRestore(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[BackupRestoreRecord], error) {
	var zero etcdstore.Versioned[BackupRestoreRecord]
	if ids.Validate(ids.KindTask, taskID) != nil {
		return zero, invalidBackupRuntimeRecord("restore Task identity is invalid")
	}
	record, found, err := getOptionalBackupRuntimeRecord(ctx, reader.store, BackupRestoreKey(taskID), taskID,
		DecodeBackupRestoreRecord, func(value BackupRestoreRecord) string { return value.TaskID })
	if err != nil {
		return zero, err
	}
	if !found {
		return zero, errs.New(errs.KindTaskNotFound, "restore execution record was not found")
	}
	key, err := BackupRestoreEnvironmentIndexKey(record.Record.EnvironmentID, taskID)
	if err != nil {
		return zero, err
	}
	primaryKey := BackupRestoreKey(taskID)
	read, err := reader.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{primaryKey, key}, Revision: record.ReadRevision},
	)
	if err != nil {
		return zero, err
	}
	if read == nil || read.ReadRevision != record.ReadRevision || len(read.Values) != 2 || read.Values[0] == nil ||
		read.Values[1] == nil {
		return zero, CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	primary, member := read.Values[0], read.Values[1]
	stored, decodeErr := DecodeBackupRestoreRecord(primary.Value)
	if decodeErr != nil || primary.Key != primaryKey || primary.Version <= 0 || primary.ModRevision <= 0 ||
		primary.ModRevision != record.Revision || !BackupRestoreRecordsEqual(stored, record.Record) {
		return zero, CorruptBackupRuntimeRecord()
	}
	if member.Key != key || member.Version != 1 || member.ModRevision <= 0 || member.ModRevision > record.Revision ||
		string(member.Value) != taskID {
		return zero, CorruptBackupRuntimeRecord()
	}
	return record, nil
}
