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

func (repository *InputRepository) PrepareCaptureRetention(ctx context.Context, view workqueue.View,
	at time.Time, final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionBackup || task.ExpectedItems != 1 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) ||
		at.Before(*task.RetainUntil) {
		return false, workqueue.Change{}, conflict("Backup history requires expired settled execution")
	}
	input, inputGuard, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	ready, run, guards, err := repository.captureRetentionGuards(ctx, view, input)
	if err != nil || !ready {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(guards, inputGuard,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})}
	ready, hierarchy, err := repository.retentionHierarchyGuards(ctx, view)
	if err != nil || !ready {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, hierarchy...)
	if !final {
		return true, change, nil
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true},
	)
	indexes, err := repository.prepareStagingIndexRetirement(ctx, view, input)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, indexes.Conditions...)
	change.Mutations = append(change.Mutations, indexes.Mutations...)
	for _, source := range run.Sources {
		if source.Snapshot.Config == nil {
			continue
		}
		if source.Snapshot.Config.ConfigSnapshotID != task.ID {
			return false, workqueue.Change{}, conflict("Backup snapshot is not exclusively owned")
		}
		conditions, mutations, err := backupconfiguration.PrepareCaptureRetirement(ctx, repository.store,
			task.ID, run.EnvironmentID, source.SourceID, view.ReadRevision)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, conditions...)
		change.Mutations = append(change.Mutations, mutations...)
	}
	membership, err := backupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, task.ID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	membershipGuards, err := backupruntime.HistoryMembershipRetentionGuards(ctx, repository.store,
		backupruntime.BackupOperationBackup, run.EnvironmentID, task.ID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, membershipGuards...)
	for _, key := range []string{InputKey(item.ID), ProgressKey(item.ID), RecoveryKey(item.ID), backupruntime.BackupRunKey(task.ID), membership} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	for _, prefix := range []string{checkpointPrefix(item.ID), credentialGrantPrefix(item.ID), backupconfiguration.ConfigTransferTaskPrefix(task.ID)} {
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: prefix, Prefix: true},
		)
	}
	return true, change, nil
}
