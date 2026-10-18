package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadRestoreSource returns the selected resource record at the execution
// snapshot, never a newer Point or a reconstructed diagnostic-history record.
func (repository *InputRepository) LoadRestoreSource(ctx context.Context, view workqueue.ResumeView,
) (backupruntime.BackupRestoreRecord, *agentpb.BackupWorkAssignment, workqueue.Change, error) {
	var zero backupruntime.BackupRestoreRecord
	assignment, guards, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return zero, nil, workqueue.Change{}, err
	}
	key := backupruntime.BackupRestoreKey(view.Work.Task.Task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return zero, nil, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return zero, nil, workqueue.Change{}, conflict("Restore source record is unavailable")
	}
	record, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil {
		return zero, nil, workqueue.Change{}, err
	}
	if record.TaskID != view.Work.Task.Task.ID {
		return zero, nil, workqueue.Change{}, conflict("Restore source belongs to another Task")
	}
	if err := backupruntime.ValidateRestoreWorkInput(record, assignment.Input); err != nil {
		return zero, nil, workqueue.Change{}, err
	}
	guards.Conditions = append(guards.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return record, assignment, guards, nil
}
