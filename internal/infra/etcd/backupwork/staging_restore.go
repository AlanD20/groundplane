package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *InputRepository) LoadStagingRestore(
	ctx context.Context,
	source StagingSource,
) (backupruntime.BackupRestoreRecord, keyvalue.Condition, error) {
	var zero backupruntime.BackupRestoreRecord
	if source.Input == nil || source.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE {
		return zero, keyvalue.Condition{}, conflict("Source staging is not a captured Restore")
	}
	key := backupruntime.BackupRestoreKey(source.View.Task.Task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, source.View.ReadRevision)
	if err != nil {
		return zero, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return zero, keyvalue.Condition{}, conflict("Restore source record is unavailable")
	}
	record, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil {
		return zero, keyvalue.Condition{}, err
	}
	if record.TaskID != source.View.Task.Task.ID {
		return zero, keyvalue.Condition{}, conflict("Restore source belongs to another Task")
	}
	if err := backupruntime.ValidateRestoreWorkInput(record, source.Input); err != nil {
		return zero, keyvalue.Condition{}, err
	}
	return record, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
