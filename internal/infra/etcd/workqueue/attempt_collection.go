package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareAttemptCollection retires one finished attempt, never a worker slot or
// resource claim. The caller joins deletion of operation-owned result evidence
// in this same transaction, after the operation's captured input was retired.
func PrepareAttemptCollection(task TaskSnapshot, item ItemSnapshot, attempt AttemptSnapshot) (Change, error) {
	if task.Revision <= 0 || item.Revision <= 0 || attempt.Revision <= 0 ||
		task.Task.Validate() != nil || item.Item.Validate() != nil || attempt.Attempt.Validate() != nil ||
		task.Task.CollectionStartedAt == nil || item.Item.TaskID != task.Task.ID ||
		item.Item.Phase != workexecution.Settled || item.Item.ActiveAttemptID != "" ||
		attempt.Attempt.TaskID != task.Task.ID || attempt.Attempt.WorkItemID != item.Item.ID ||
		attempt.Attempt.Number > item.Item.AttemptNumber || attempt.Attempt.FinishedAt == nil {
		return Change{}, invalid("attempt collection requires expired, settled work")
	}
	return Change{
		Conditions: []keyvalue.Condition{
			{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
			{Key: ItemKey(item.Item.ID), ModRevision: item.Revision},
			{Key: AttemptKey(attempt.Attempt.ID), ModRevision: attempt.Revision},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: AttemptKey(attempt.Attempt.ID)},
			{Type: keyvalue.MutationDelete, Key: AttemptIndexKey(item.Item.ID, attempt.Attempt.Number)},
			{Type: keyvalue.MutationDelete, Key: CheckpointKey(attempt.Attempt.ID)},
		},
	}, nil
}
