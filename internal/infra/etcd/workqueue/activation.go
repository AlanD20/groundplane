package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareActivation orders native software activation against cancellation.
// Join the operation's immutable handoff record and its prerequisite comparisons
// in this transaction. Only a successful commit allows the first handoff; an
// uncertain commit must be reconciled, never retried as a new external effect.
func PrepareActivation(task TaskSnapshot, item ItemSnapshot, attempt AttemptSnapshot,
	executor ExecutorSnapshot, input workexecution.Input, at time.Time,
) (Change, error) {
	if item.Revision <= 0 || attempt.Revision <= 0 || executor.Revision <= 0 ||
		!executor.Record.Ready || executor.Record.Identity != attempt.Attempt.Executor {
		return Change{}, invalid("software activation requires the current execution owner")
	}
	if err := executor.Record.Validate(); err != nil {
		return Change{}, err
	}
	next, err := workexecution.AuthorizeActivation(task.Task, item.Item, attempt.Attempt, input, at)
	if err != nil {
		return Change{}, err
	}
	change, err := taskChange(task, next)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: ItemKey(item.Item.ID), ModRevision: item.Revision},
		keyvalue.Condition{Key: AttemptKey(attempt.Attempt.ID), ModRevision: attempt.Revision},
		keyvalue.Condition{Key: ExecutorKey(executor.Record.Identity), ModRevision: executor.Revision})
	return change, nil
}
