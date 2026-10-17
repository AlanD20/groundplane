package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PreparePublicationSettlement accounts for one rejected, never-assigned member
// of a closed graph. A zero item revision means this captured member has not yet
// been staged. The caller must join captured-graph authority, immutable rejection
// evidence and any operation-owned staging cleanup; no resource is unlocked here.
func PreparePublicationSettlement(task TaskSnapshot, item ItemSnapshot, result workexecution.Completion,
	at time.Time,
) (Change, error) {
	if task.Revision <= 0 || item.Revision < 0 {
		return Change{}, invalid("publication settlement requires accepted Task ownership")
	}
	nextTask, nextItem, err := workexecution.SettlePublication(task.Task, item.Item, item.Revision > 0, result, at)
	if err != nil {
		return Change{}, err
	}
	change, err := taskChange(task, nextTask)
	if err != nil {
		return Change{}, err
	}
	raw, err := EncodeItem(nextItem)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ItemKey(nextItem.ID), ModRevision: item.Revision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ItemKey(nextItem.ID), Value: raw})
	if item.Revision == 0 {
		reference, err := encode("execution_owner", ClaimOwner{TaskID: nextItem.TaskID, WorkItemID: nextItem.ID})
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: TaskItemKey(nextItem.TaskID, nextItem.ID)},
			keyvalue.Condition{Key: QueueKey(nextItem)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: TaskItemKey(nextItem.TaskID, nextItem.ID), Value: reference})
	} else {
		if item.Item.Phase == workexecution.Ready {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: QueueKey(nextItem)})
		}
		if err := updateInputQueue(&change, item.Item, nextItem); err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return Change{}, err
		}
	}
	return change, nil
}

// PrepareRejectedPublicationComplete closes publication after every planned item
// has a durable no-execution outcome. The caller must read all membership at one
// revision and compare its captured graph and operation cleanup. Dispatch sees
// only settled items; ordinary aggregate completion then retains the failure.
func PrepareRejectedPublicationComplete(task TaskSnapshot, items []workexecution.Item, at time.Time) (Change, error) {
	if task.Revision <= 0 || task.Task.PublicationComplete || task.Task.FinishedAt != nil ||
		at.Before(task.Task.UpdatedAt) || task.Task.Progress.Unsettled() != 0 ||
		task.Task.Progress.Total() != uint64(task.Task.ExpectedItems) || task.Task.Outcome == "" ||
		task.Task.Outcome == workexecution.Succeeded {
		return Change{}, invalid("rejected publication still has unfinished captured work")
	}
	if err := workexecution.ValidateGraph(task.Task, items); err != nil {
		return Change{}, err
	}
	var progress workexecution.Progress
	for _, item := range items {
		if item.Phase != workexecution.Settled || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
			item.Result == nil || item.Cleanup != workexecution.CleanupNone || item.CleanupResult != nil {
			return Change{}, invalid("rejected publication contains work requiring execution reconciliation")
		}
		switch item.Outcome {
		case workexecution.Failed:
			progress.Failed++
		case workexecution.Cancelled:
			progress.Cancelled++
		case workexecution.Superseded:
			progress.Superseded++
		default:
			return Change{}, invalid("rejected publication cannot claim successful execution")
		}
	}
	if progress != task.Task.Progress {
		return Change{}, invalid("rejected publication differs from its stored progress")
	}
	next := workexecution.CloneTask(task.Task)
	next.PublicationComplete, next.UpdatedAt = true, at
	return taskChange(task, next)
}
