package operationretention

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (collector *Collector) collectHierarchy(ctx context.Context, task workqueue.TaskSnapshot, revision int64, guard etcd.HierarchyCollection, at time.Time) (bool, error) {
	if !guard.Eligible {
		return false, nil
	}
	if task.Task.CollectionStartedAt == nil {
		change, err := workqueue.PrepareCollection(task, at)
		if err != nil {
			return false, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		_, err = collector.queue.Commit(ctx, change, guard.Authority)
		return false, err
	}
	page, err := collector.queue.ListWork(ctx, task.Task.ID, "", 1, revision)
	if err != nil {
		return false, err
	}
	if len(page.Items) != 0 {
		return collector.collectOwnedWork(ctx, task.Task.ID, page.Items[0].Item.ID, at)
	}
	if guard.PlanningRejection {
		done, artifacts, err := collector.hierarchy.PreparePlanningArtifactCollection(ctx, task, guard, revision)
		if err != nil {
			return false, err
		}
		if !done {
			_, err := collector.queue.Commit(ctx, artifacts)
			return false, err
		}
		guard.Authority.Conditions = artifacts.Conditions
	}
	change, err := etcd.PrepareHierarchyGraphCollection(task, guard)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.TaskEventsKey(task.Task.ID)})
	result, err := collector.queue.Commit(ctx, change)
	return result.Succeeded, err
}
