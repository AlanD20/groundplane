package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Execution has already settled resource effects atomically. Aggregate Task
// completion checks that durable resource outcome instead of running it again.
func (repository *InputRepository) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action == workexecution.ActionBackupPrune {
		return repository.PreparePruneFinish(ctx, finished)
	}
	operation, key := workexecution.Backup, backupruntime.BackupRunKey(task.ID)
	switch task.Action {
	case workexecution.ActionBackup:
	case workexecution.ActionRestore:
		operation, key = workexecution.Restore, backupruntime.BackupRestoreKey(task.ID)
	default:
		return workqueue.Change{}, conflict("Backup completion action is unsupported")
	}
	if task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, conflict("Backup completion requires one settled work item")
	}
	item := finished.Items[0]
	if item.Item.Operation != operation || item.Item.TaskID != task.ID || item.Item.TargetID != task.TargetID ||
		item.Item.Phase != workexecution.Settled || item.Item.Result == nil || item.Item.ActiveAttemptID != "" ||
		(item.Item.Cleanup != workexecution.CleanupNone && item.Item.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, conflict("Backup work still has unresolved execution or cleanup")
	}
	input, guard, err := repository.Load(
		ctx,
		workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	read, err := repository.readSettlementKeys(ctx, []string{key}, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Backup resource outcome is unavailable")
	}
	if operation == workexecution.Backup {
		run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		if run.TaskID != task.ID || run.EnvironmentID != input.EnvironmentId ||
			!backupruntime.TerminalBackupRunState(run.State) ||
			(item.Item.Outcome == workexecution.Succeeded && run.State != backupruntime.BackupRunCompleted) {
			return workqueue.Change{}, conflict("Backup resource has not reached its settled outcome")
		}
	} else {
		restore, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		if restore.TaskID != task.ID || restore.EnvironmentID != input.EnvironmentId ||
			(restore.State != backupruntime.BackupRestoreCompleted && restore.State != backupruntime.BackupRestoreFailedSafe) ||
			(item.Item.Outcome == workexecution.Succeeded && restore.State != backupruntime.BackupRestoreCompleted) {
			return workqueue.Change{}, conflict("Restore resource has not reached its settled outcome")
		}
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{guard,
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
		{Key: key, ModRevision: read.Values[0].ModRevision}}}, nil
}
