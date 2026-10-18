package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PreparePruneRetention never deletes Recovery Points or pending deletion
// records. Those are resource obligations, not retained Task diagnostics.
func (repository *InputRepository) PreparePruneRetention(ctx context.Context, view workqueue.View,
	at time.Time, final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		task.Action != workexecution.ActionBackupPrune || item.Operation != workexecution.BackupPrune ||
		item.TaskID != task.ID || item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		(item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete) {
		return false, workqueue.Change{}, conflict("Backup prune history requires expired and fully settled execution")
	}
	_, input, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	dispatchKey := backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{dispatchKey}, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		return false, workqueue.Change{}, nil
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{input,
		{Key: dispatchKey},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}}}
	if final {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: InputKey(item.ID)},
			{Type: keyvalue.MutationDelete, Key: ProgressKey(item.ID)},
			{Type: keyvalue.MutationDelete, Key: RecoveryKey(item.ID)},
			{Type: keyvalue.MutationDelete, Key: checkpointPrefix(item.ID), Prefix: true},
			{Type: keyvalue.MutationDelete, Key: credentialGrantPrefix(item.ID), Prefix: true},
		}
	}
	return true, change, nil
}

func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if (item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) ||
		item.Input == nil || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, conflict("Backup attempt history differs from its captured input or host")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: FailureKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: RecoveryReportKey(attempt.ID)},
	}}, nil
}
