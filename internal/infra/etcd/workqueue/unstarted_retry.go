package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func PrepareUnstartedRetry(task TaskSnapshot, item ItemSnapshot, deadline, at time.Time) (Change, error) {
	next, err := workexecution.RetryUnstarted(item.Item, deadline, at)
	if err != nil {
		return Change{}, err
	}
	change, err := prepareReadyChange(item, next)
	if err != nil {
		return Change{}, err
	}
	updated, err := workexecution.CountRetry(task.Task, item.Item, next, at)
	if err != nil {
		return Change{}, err
	}
	parent, err := taskChange(task, updated)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, parent.Conditions...)
	change.Mutations = append(change.Mutations, parent.Mutations...)
	return change, nil
}

func PrepareUnstartedDeadline(task TaskSnapshot, item ItemSnapshot, queueRevision int64, at time.Time) (Change, error) {
	if task.Revision <= 0 || item.Revision <= 0 || queueRevision <= 0 || !task.Task.PublicationComplete ||
		task.Task.Stop != nil || task.Task.FinishedAt != nil || task.Task.CollectionStartedAt != nil || item.Item.TaskID != task.Task.ID {
		return Change{}, invalid("queued deadline renewal requires its active Task and queue entry")
	}
	next, err := workexecution.RenewUnstartedDeadline(item.Item, task.Task.Deadline, at)
	if err != nil {
		return Change{}, err
	}
	value, err := EncodeItem(next)
	if err != nil {
		return Change{}, err
	}
	return Change{Conditions: []keyvalue.Condition{
		{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: ItemKey(item.Item.ID), ModRevision: item.Revision},
		{Key: QueueKey(item.Item), ModRevision: queueRevision},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ItemKey(next.ID), Value: value}}}, nil
}
