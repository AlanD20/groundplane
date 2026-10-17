package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareInput joins exact dependency-output comparisons and immutable input
// publication supplied by the operation owner. It never selects newer intent.
func PrepareInput(task TaskSnapshot, item ItemSnapshot, input workexecution.Input, at time.Time) (Change, error) {
	next, err := workexecution.PrepareInput(item.Item, input, at)
	if err != nil {
		return Change{}, err
	}
	change, err := prepareReadyChange(item, next)
	if err != nil {
		return Change{}, err
	}
	if err := countTransition(&change, task, item.Item, next, at); err != nil {
		return Change{}, err
	}
	return change, nil
}

// PrepareRetry must be committed with the operation's retry authorization and
// request replay marker. Reading a failed Task alone is not that authorization.
func PrepareRetry(task TaskSnapshot, item ItemSnapshot, previous AttemptSnapshot,
	decision workexecution.RetryDecision, at time.Time,
) (Change, error) {
	if previous.Revision <= 0 {
		return Change{}, invalid("retry requires the saved preceding attempt")
	}
	next, err := workexecution.Retry(item.Item, previous.Attempt, decision, at)
	if err != nil {
		return Change{}, err
	}
	change, err := prepareReadyChange(item, next)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{
		Key: AttemptKey(previous.Attempt.ID), ModRevision: previous.Revision,
	})
	updatedTask, err := workexecution.CountRetry(task.Task, item.Item, next, at)
	if err != nil {
		return Change{}, err
	}
	parent, err := taskChange(task, updatedTask)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, parent.Conditions...)
	change.Mutations = append(change.Mutations, parent.Mutations...)
	return change, nil
}

func PrepareRetryReady(task TaskSnapshot, item ItemSnapshot, queueRevision int64, at time.Time) (Change, error) {
	if item.Revision <= 0 || queueRevision <= 0 {
		return Change{}, invalid("retry wake-up requires its persisted queue entry")
	}
	next, err := workexecution.ReleaseRetryWait(item.Item, at)
	if err != nil {
		return Change{}, err
	}
	value, err := EncodeItem(next)
	if err != nil {
		return Change{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{{Key: ItemKey(next.ID), ModRevision: item.Revision},
			{Key: QueueKey(next), ModRevision: queueRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ItemKey(next.ID), Value: value}},
	}
	if err := countTransition(&change, task, item.Item, next, at); err != nil {
		return Change{}, err
	}
	return change, nil
}

func prepareReadyChange(before ItemSnapshot, next workexecution.Item) (Change, error) {
	if before.Revision <= 0 || before.Item.ID != next.ID || before.Item.TaskID != next.TaskID {
		return Change{}, invalid("work readiness requires its current record")
	}
	value, err := EncodeItem(next)
	if err != nil {
		return Change{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{
			{Key: ItemKey(next.ID), ModRevision: before.Revision}, {Key: QueueKey(next)},
		},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ItemKey(next.ID), Value: value}},
	}
	if next.Phase == workexecution.Ready || retryWaiting(next) {
		reference, err := encode("execution_owner", ClaimOwner{TaskID: next.TaskID, WorkItemID: next.ID})
		if err != nil {
			return Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: QueueKey(next), Value: reference})
	}
	return change, nil
}

func PrepareCleanupRetryExpiry(task TaskSnapshot, item ItemSnapshot, at time.Time) (Change, error) {
	if item.Revision <= 0 {
		return Change{}, invalid("cleanup retry expiry requires current work")
	}
	next, err := workexecution.ExpireCleanupRetry(item.Item, at)
	if err != nil {
		return Change{}, err
	}
	value, err := EncodeItem(next)
	if err != nil {
		return Change{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{{Key: ItemKey(next.ID), ModRevision: item.Revision}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ItemKey(next.ID), Value: value}},
	}
	if item.Item.Phase == workexecution.Ready || retryWaiting(item.Item) {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: QueueKey(next)})
	}
	if err := countTransition(&change, task, item.Item, next, at); err != nil {
		return Change{}, err
	}
	return change, nil
}

func retryWaiting(item workexecution.Item) bool {
	return item.Phase == workexecution.Waiting && item.Wait != nil && item.Wait.Reason == workexecution.WaitRetry
}
