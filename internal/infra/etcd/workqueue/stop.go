package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareStop closes the forward gate for every item, including items not yet
// published. Comparing this same Task record in assignment/start prevents a
// partially processed Abort from letting later items begin.
func PrepareStop(task TaskSnapshot, outcome workexecution.Outcome, at time.Time) (Change, error) {
	next, err := workexecution.RequestStop(task.Task, outcome, at)
	if err != nil {
		return Change{}, err
	}
	change, err := taskChange(task, next)
	if err != nil || task.Task.Stop != nil {
		return change, err
	}
	cursor := StopCursor{TaskID: task.Task.ID}
	value, err := encode("execution_stop", cursor)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: stopKey(task.Task.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: stopKey(task.Task.ID), Value: value},
	)
	return change, nil
}

// PrepareRevocation is the per-resource settlement following Abort or latest-wins.
// An active assignment, its slot, checkpoints and claims remain untouched.
// For supersession of one item, the operation owner joins the newer intent's
// revision; whole-Task Abort uses the Task's persisted Stop request.
func PrepareRevocation(
	task TaskSnapshot,
	item ItemSnapshot,
	outcome workexecution.Outcome,
	at time.Time,
) (Change, error) {
	if item.Revision <= 0 || item.Item.Phase == workexecution.Settled {
		return Change{}, invalid("revocation requires unsettled revisioned work")
	}
	if task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	}

	// An input-less item has no executable result to retain. Publish its native
	// preparation settlement with revocation, rather than leaving a terminal
	// item whose operation owner can never load completion evidence.
	if waitingInput(item.Item) && item.Item.AttemptNumber == 0 && item.Item.ActiveAttemptID == "" &&
		item.Item.Cleanup == workexecution.CleanupNone {
		view := View{Task: task, Item: item, ReadRevision: max(task.Revision, item.Revision)}
		if task.Task.Stop != nil {
			return PrepareInputSettlement(view, at)
		}
		if outcome != workexecution.Cancelled && outcome != workexecution.Superseded {
			return Change{}, invalid("unprepared work revocation requires cancellation or supersession")
		}
		reason := "Cancelled before executable input was published"
		if outcome == workexecution.Superseded {
			reason = "Superseded before executable input was published"
		}
		return prepareInputSettlement(view, InputSettlement{TaskID: task.Task.ID, WorkID: item.Item.ID,
			Outcome: outcome, Reason: reason, At: at})
	}
	next, err := workexecution.RevokeForward(item.Item, outcome, at)
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
	if (item.Item.Phase == workexecution.Ready || retryWaiting(item.Item)) && next.Phase == workexecution.Settled {
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: QueueKey(next)},
		)
	}
	if err := countTransition(&change, task, item.Item, next, at); err != nil {
		return Change{}, err
	}
	return change, nil
}
