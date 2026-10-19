package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Restore history owns transfer receipts, not the published Blueprint or Entry
// generations. Those remain resource state after the execution is collected.
func (repository *InputRepository) PrepareRestoreRetention(ctx context.Context, view workqueue.View,
	at time.Time, final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionRestore ||
		task.ExpectedItems != 1 || item.Operation != workexecution.Restore || item.TaskID != task.ID ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		(item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete) {
		return false, workqueue.Change{}, conflict("Restore history requires expired and fully settled execution")
	}
	input, inputGuard, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	key := backupruntime.BackupRestoreKey(task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, workqueue.Change{}, conflict("Restore retention record is unavailable")
	}
	record, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if record.TaskID != task.ID || record.EnvironmentID != task.Scope.EnvironmentID {
		return false, workqueue.Change{}, conflict("Restore retention record belongs to another execution")
	}
	if err := backupruntime.ValidateRestoreWorkInput(record, input); err != nil {
		return false, workqueue.Change{}, err
	}
	if record.State != backupruntime.BackupRestoreCompleted && record.State != backupruntime.BackupRestoreFailedSafe {
		return false, workqueue.Change{}, nil
	}
	var environments []string
	if record.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
		environment, found, err := backupruntime.DatabaseRestoreBackingEnvironmentID(record)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		if !found {
			return false, workqueue.Change{}, conflict("Restore backing retention identity is missing")
		}
		environments = append(environments, environment)
	}
	ready, backingGuards, err := repository.retentionBackingGuards(
		ctx,
		view,
		environments,
		record.OperationID,
		backupruntime.BackupOperationRestore,
	)
	if err != nil || !ready {
		return false, workqueue.Change{}, err
	}
	ready, guards, err := repository.retentionHierarchyGuards(ctx, view)
	if err != nil || !ready {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(guards, inputGuard,
		keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})}
	change.Conditions = append(change.Conditions, backingGuards...)
	if !final {
		return true, change, nil
	}
	indexes, err := repository.prepareStagingIndexRetirement(ctx, view, input)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, indexes.Conditions...)
	change.Mutations = append(change.Mutations, indexes.Mutations...)
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(record.EnvironmentID, task.ID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	membershipGuards, err := backupruntime.HistoryMembershipRetentionGuards(ctx, repository.store,
		backupruntime.BackupOperationRestore, record.EnvironmentID, task.ID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, membershipGuards...)
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
	for _, owned := range []string{key, membership, InputKey(item.ID), ProgressKey(item.ID), RecoveryKey(item.ID)} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: owned})
	}
	for _, prefix := range []string{checkpointPrefix(item.ID), credentialGrantPrefix(item.ID), backupconfiguration.ConfigTransferTaskPrefix(task.ID)} {
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: prefix, Prefix: true},
		)
	}
	return true, change, nil
}
