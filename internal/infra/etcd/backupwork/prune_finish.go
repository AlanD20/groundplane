package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) PreparePruneFinish(ctx context.Context, finished workqueue.FinishedWork) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionBackupPrune || task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, conflict("Backup prune requires one settled deletion operation")
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.BackupPrune || item.Item.TaskID != task.ID || item.Item.TargetID != task.TargetID ||
		item.Item.Phase != workexecution.Settled || item.Item.Result == nil || item.Item.ActiveAttemptID != "" ||
		(item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, conflict("Backup prune still has unresolved execution or cleanup")
	}
	input, guard, err := repository.Load(ctx, workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.EnvironmentId != task.Scope.EnvironmentID {
		return workqueue.Change{}, conflict("Backup prune result belongs to a different Environment")
	}
	key := backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		return workqueue.Change{}, conflict("Backup prune still owns deletion selections")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{guard,
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision}, {Key: key}}}, nil
}
