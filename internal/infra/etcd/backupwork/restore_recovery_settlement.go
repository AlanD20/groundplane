package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Physical source cleanup does not prove restoration of live data. Release
// only when the mutation gate proves no live effects were authorized or exact
// resource receipts already prove the complete restore and consumer recovery.
func (repository *InputRepository) prepareRestoreRecoverySettlement(ctx context.Context, view workqueue.View,
	input *agentpb.BackupWorkInput, at time.Time,
) (workqueue.Change, error) {
	key := backupruntime.BackupRestoreKey(view.Task.Task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Restore cleanup record is unavailable")
	}
	record, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if record.TaskID != view.Task.Task.ID {
		return workqueue.Change{}, conflict("Restore cleanup belongs to another Task")
	}
	if err := backupruntime.ValidateRestoreWorkInput(record, input); err != nil {
		return workqueue.Change{}, err
	}
	next := record
	if record.MutationStarted {
		next, err = backupruntime.CompleteProvenRestoreRecovery(record, at)
		if err != nil {
			return workqueue.Change{}, err
		}
	} else if record.State != backupruntime.BackupRestoreFailedSafe {
		switch record.Point.SourceKind {
		case backupruntime.BackupRuntimeSourceConfig:
			next, err = backupruntime.FailConfigRestoreBeforeMutation(record, at)
		case backupruntime.BackupRuntimeSourceVolume:
			next, err = backupruntime.FailVolumeRestoreBeforeMutation(record, at)
		case backupruntime.BackupRuntimeSourceAttach:
			next, err = backupruntime.FailDatabaseRestoreBeforeMutation(record, at)
		default:
			return workqueue.Change{}, conflict("Restore cleanup source is unsupported")
		}
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}}
	return repository.prepareRestoreRelease(ctx, view, record, next, change, next.State == backupruntime.BackupRestoreCompleted)
}
