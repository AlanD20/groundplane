package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareUnstartedSettlement joins the operation owner's immutable evidence and
// resource comparisons. No attempt or worker slot is invented for Controller
// decisions that make host execution unnecessary.
func PrepareUnstartedSettlement(task TaskSnapshot, item ItemSnapshot, result workexecution.Completion, at time.Time) (Change, error) {
	if item.Revision <= 0 {
		return Change{}, invalid("unstarted settlement requires current work")
	}
	next, err := workexecution.SettleUnstarted(item.Item, result, at)
	if err != nil {
		return Change{}, err
	}
	return prepareQueuedSettlement(task, item, next, at)
}

func PrepareQueuedRetrySettlement(task TaskSnapshot, item ItemSnapshot, result workexecution.Completion, at time.Time) (Change, error) {
	if item.Revision <= 0 {
		return Change{}, invalid("retry settlement requires current work")
	}
	next, err := workexecution.SettleQueuedRetry(item.Item, result, at)
	if err != nil {
		return Change{}, err
	}
	return prepareQueuedSettlement(task, item, next, at)
}

func prepareQueuedSettlement(task TaskSnapshot, item ItemSnapshot, next workexecution.Item, at time.Time) (Change, error) {
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
	// Unassigned work has no claims: initial assignment has not acquired them,
	// and admission of a forward retry required previous effects to be settled.
	// Never release claims belonging to another operation here.
	if err := countTransition(&change, task, item.Item, next, at); err != nil {
		return Change{}, err
	}
	return change, nil
}
