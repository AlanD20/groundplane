package servicelifecyclework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRetention keeps recovery inputs until all attempts are collected. It
// never deletes Service state, applied runtime, containers or application data.
func (repository *Repository) PrepareRetention(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, conflict("Service lifecycle history requires expired, settled execution")
	}
	captured, capture, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{capture,
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
	}}
	if !final {
		return true, change, nil
	}
	collection, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, collection.Conditions...)
	if hook := captured.HookInputs; hook != nil {
		retired, guards, err := repository.retireHookPins(ctx, view.ReadRevision, captured, change.Conditions)
		if err != nil || !retired {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, guards...)
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
			Key: taskconfiguration.BackingHookTaskInputKey(hook.OperationID)})
	}
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: InputKey(item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: RecoveryKey(item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: ContinuationPrefix(item.ID), Prefix: true},
	)
	return true, change, nil
}
