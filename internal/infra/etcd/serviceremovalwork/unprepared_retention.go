package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// An Apply can stop while configuration Secrets are being staged, before it
// publishes executable input. Retirement must collect that staging without
// demanding an input that never existed or touching another removal's state.
func (repository *Repository) retireUnprepared(
	ctx context.Context,
	view workqueue.View,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || item.AttemptNumber != 0 || item.Input != nil ||
		item.Outcome == workexecution.Succeeded || item.Cleanup != workexecution.CleanupNone {
		return false, workqueue.Change{}, conflict("Service removal without input cannot have execution history")
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: InputKey(item.ID)},
	}}
	if !final {
		return true, change, nil
	}
	collection, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, collection.Conditions...)
	operationID := ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "removal-configuration/"+item.ID)
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, task.Scope.ProjectID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	guards := make([]tasksecretpins.Condition, 0, len(change.Conditions))
	for _, guard := range change.Conditions {
		guards = append(
			guards,
			tasksecretpins.Condition{Key: guard.Key, ModRevision: guard.ModRevision, Prefix: guard.Prefix},
		)
	}
	done, err := pins.AbandonOwned(ctx, operationID, task.ID, item.ID, guards)
	if err != nil || !done {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: tasksecretpins.RootKey(operationID)},
		keyvalue.Condition{Key: tasksecretpins.PreparationKey(operationID)},
		keyvalue.Condition{Key: tasksecretpins.ReversePrefix(operationID), Prefix: true})
	return true, change, nil
}
