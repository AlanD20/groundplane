package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareWorkCollection retires one settled item after its attempts and owned
// inputs are gone. The caller joins operation-specific absence comparisons.
// Counters remain the historical outcome, not the number of retained records.
func PrepareWorkCollection(task TaskSnapshot, item ItemSnapshot) (Change, error) {
	if task.Revision <= 0 || item.Revision <= 0 || task.Task.Validate() != nil || item.Item.Validate() != nil ||
		task.Task.CollectionStartedAt == nil || item.Item.TaskID != task.Task.ID ||
		item.Item.Phase != workexecution.Settled || item.Item.ActiveAttemptID != "" {
		return Change{}, invalid("work collection requires an expired settled Task and item")
	}
	return Change{
		Conditions: []keyvalue.Condition{
			{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
			{Key: ItemKey(item.Item.ID), ModRevision: item.Revision},
			{Key: AttemptIndexPrefix(item.Item.ID), Prefix: true},
			{Key: QueueKey(item.Item)},
			{Key: inputQueueKey(item.Item.ID)},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: ItemKey(item.Item.ID)},
			{Type: keyvalue.MutationDelete, Key: TaskItemKey(task.Task.ID, item.Item.ID)},
			{Type: keyvalue.MutationDelete, Key: inputSettlementKey(item.Item.ID)},
		},
	}, nil
}

// PrepareTaskCollection is the final collection barrier for any graph size.
// No work membership may remain. The aggregate owner joins retirement of its
// captured plan and other Task-owned records; current resource state is separate.
func PrepareTaskCollection(task TaskSnapshot) (Change, error) {
	if task.Revision <= 0 || task.Task.Validate() != nil || task.Task.CollectionStartedAt == nil ||
		task.Task.RetainUntil == nil || task.Task.FinishedAt == nil || task.Task.Progress.Unsettled() != 0 {
		return Change{}, invalid("Task collection requires expired fully settled work")
	}
	change := Change{
		Conditions: []keyvalue.Condition{
			{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
			{Key: TaskItemsPrefix(task.Task.ID), Prefix: true},
			{Key: taskPublishingKey(task.Task.ID)},
			{Key: taskFinishKey(task.Task.ID)},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: TaskKey(task.Task.ID)},
			{Type: keyvalue.MutationDelete, Key: replayBindingKey(task.Task.ID)},
			{Type: keyvalue.MutationDelete, Key: retentionKey(task.Task.ID, *task.Task.RetainUntil)},
			{Type: keyvalue.MutationDelete, Key: stopKey(task.Task.ID)},
		},
	}
	for _, key := range taskListKeys(task.Task) {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	return change, nil
}
