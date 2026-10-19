package operationretention

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (collector *Collector) collectZone(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
	at time.Time,
) (bool, error) {
	eligible, guard, err := collector.networks.CollectionGuard(ctx, task, revision)
	if err != nil || !eligible {
		return false, err
	}
	if task.Task.CollectionStartedAt == nil {
		change, err := workqueue.PrepareCollection(task, at)
		if err != nil {
			return false, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		_, err = collector.queue.Commit(ctx, change, guard)
		return false, err
	}
	page, err := collector.queue.ListWork(ctx, task.Task.ID, "", 1, revision)
	if err != nil {
		return false, err
	}
	if len(page.Items) != 0 {
		return collector.collectOwnedWork(ctx, task.Task.ID, page.Items[0].Item.ID, at)
	}
	change, err := networkwork.PrepareZoneTaskCollection(task, guard)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
		Key: executiondiagnostics.TaskEventsKey(task.Task.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.TaskExpiryKey(task.Task.ID)},
	)
	result, err := collector.queue.Commit(ctx, change)
	return result.Succeeded, err
}
