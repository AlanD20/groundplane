package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// A surviving backing lock is an outstanding resource obligation, even if the
// visible execution is terminal. Collection never releases that lock itself.
func (repository *InputRepository) retentionBackingGuards(ctx context.Context, view workqueue.View,
	environments []string, operationID string, kind backupruntime.BackupOperationKind,
) (bool, []keyvalue.Condition, error) {
	if len(environments) == 0 {
		return true, nil, nil
	}
	keys := make([]string, 0, len(environments))
	seen := make(map[string]bool, len(environments))
	for _, environment := range environments {
		key := hierarchy.EnvironmentOperationLockKey(environment)
		if !seen[key] {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	read, err := repository.readSettlementKeys(ctx, keys, view.ReadRevision)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	guards := make([]keyvalue.Condition, 0, len(keys))
	for index, value := range read.Values {
		guard := keyvalue.Condition{Key: keys[index]}
		if value != nil {
			lock, err := backupruntime.DecodeBackupOperationLockRecord(value.Value)
			if err != nil {
				return false, nil, err
			}
			if lock.TaskID == view.Task.Task.ID && lock.OperationID == operationID && lock.Kind == kind {
				return false, nil, nil
			}
			guard.ModRevision = value.ModRevision
		}
		guards = append(guards, guard)
	}
	return true, guards, nil
}
