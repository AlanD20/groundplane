package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Current encryption material belongs to the Environment, independent of this
// operation receipt. History retention never removes the current key or identity.
func (repository *BackupPolicyRepository) PrepareBackupKeyRotationRetention(ctx context.Context,
	view workqueue.View, at time.Time, final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !final || task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) || item.AttemptNumber != 0 ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation history is not ready for collection",
		)
	}
	change, err := repository.PrepareBackupKeyRotationFinish(ctx, workqueue.FinishedWork{
		Task: view.Task, Items: []workqueue.ItemSnapshot{view.Item}, ReadRevision: view.ReadRevision})
	if err != nil {
		return false, workqueue.Change{}, err
	}
	index, err := backupruntime.BackupKeyRotationEnvironmentIndexKey(task.TargetID, task.ID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	membershipGuards, err := backupruntime.HistoryMembershipRetentionGuards(ctx, repository.store,
		backupruntime.BackupOperationRotation, task.TargetID, task.ID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, membershipGuards...)
	ancestors := []string{hierarchydeletion.HierarchyDeletionTombstoneKey("environment", task.Scope.EnvironmentID)}
	if task.Scope.ProjectID != "" {
		ancestors = append(ancestors, hierarchydeletion.HierarchyDeletionTombstoneKey("project", task.Scope.ProjectID))
		ancestors = append(
			ancestors,
			hierarchydeletion.HierarchyDeletionTombstoneKey("backing-service", task.Scope.ProjectID),
		)
	}
	if task.Scope.TenantID != "" {
		ancestors = append(ancestors, hierarchydeletion.HierarchyDeletionTombstoneKey("tenant", task.Scope.TenantID))
	}
	deletions, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: ancestors, Revision: view.ReadRevision},
	)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if deletions == nil || deletions.ReadRevision != view.ReadRevision || len(deletions.Values) != len(ancestors) {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation retention ancestry is unavailable",
		)
	}
	defer keyvalue.ClearValues(deletions.Values)
	for index, value := range deletions.Values {
		if value != nil {
			return false, workqueue.Change{}, nil
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ancestors[index]})
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: backupruntime.BackupKeyRotationKey(task.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: index})
	return true, change, nil
}
