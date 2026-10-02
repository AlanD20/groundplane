package etcd

import (
	"context"

	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// taskOrphanCleanupPruneConditions holds the original Task only until each
// surviving orphan owns an acknowledged cleanup proof. A bounded Backup run
// has at most twelve sources; final deletion repeats these CAS conditions.
func (repository *TaskRepository) taskOrphanCleanupPruneConditions(ctx context.Context,
	task TaskRecord, revision int64,
) (bool, []etcdstore.Condition, error) {
	if task.Type != taskjournal.TaskBackup {
		return false, nil, nil
	}
	runKey := backupruntime.BackupRunKey(task.ID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{runKey}, Revision: revision,
	})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return false, nil, errs.New(errs.KindStateConflict, "backup Task run retention is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
	if err != nil || run.TaskID != task.ID || run.OperationID != task.OperationID ||
		run.EnvironmentID != task.Owner.EnvironmentID || len(run.Sources) == 0 || len(run.Sources) > 12 {
		return false, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	keys := make([]string, len(run.Sources))
	for i, source := range run.Sources {
		keys[i] = backupruntime.BackupOrphanKey(source.RecoveryPointID)
	}
	orphans, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if orphans == nil || orphans.ReadRevision != revision || len(orphans.Values) != len(keys) {
		return false, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(orphans.Values)
	conditions := []etcdstore.Condition{{Key: runKey, ModRevision: read.Values[0].ModRevision}}
	for i, value := range orphans.Values {
		condition := etcdstore.Condition{Key: keys[i]}
		if value != nil {
			orphan, err := backupruntime.DecodeBackupOrphanRecord(value.Value)
			if err != nil || orphan.Target.ID != run.Sources[i].RecoveryPointID ||
				orphan.TaskID != task.ID || orphan.Target.EnvironmentID != run.EnvironmentID ||
				orphan.Target.SourceID != run.Sources[i].SourceID {
				return false, nil, backupruntime.CorruptBackupRuntimeRecord()
			}
			if orphan.CleanupProof == (backupruntime.BackupOrphanCleanupProof{}) {
				return true, nil, nil
			}
			condition.ModRevision = value.ModRevision
		}
		conditions = append(conditions, condition)
	}
	return false, conditions, nil
}
