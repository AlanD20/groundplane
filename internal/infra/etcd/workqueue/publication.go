package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// prepareItemCreation is joined to the public Task, idempotency and operation input
// publication. A work item is never independently committed ahead of its owner.
func prepareItemCreation(item workexecution.Item) (Change, error) {
	if item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting ||
		item.AttemptNumber != 0 || item.Outcome != "" || item.FirstFailure != nil ||
		item.ForwardRevoked || item.Result != nil || item.CleanupResult != nil || item.Cleanup != workexecution.CleanupNone {
		return Change{}, invalid("new work must be unexecuted and ready or waiting")
	}
	value, err := EncodeItem(item)
	if err != nil {
		return Change{}, err
	}
	reference, err := encode("execution_owner", ClaimOwner{TaskID: item.TaskID, WorkItemID: item.ID})
	if err != nil {
		return Change{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{
			{Key: ItemKey(item.ID)}, {Key: TaskItemKey(item.TaskID, item.ID)},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: ItemKey(item.ID), Value: value},
			{Type: keyvalue.MutationPut, Key: TaskItemKey(item.TaskID, item.ID), Value: reference},
		},
	}
	if item.Phase == workexecution.Ready {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: QueueKey(item)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: QueueKey(item), Value: reference})
	}
	if err := updateInputQueue(&change, workexecution.Item{}, item); err != nil {
		return Change{}, err
	}
	return change, nil
}

type SlotSnapshot struct {
	Index    uint16
	Record   Slot
	Revision int64
}

type Completion struct {
	Task       TaskSnapshot
	Item       ItemSnapshot
	Attempt    AttemptSnapshot
	Slot       SlotSnapshot
	Claims     []ClaimSnapshot
	HostEffect HostEffectSnapshot
	Result     workexecution.Completion
	At         time.Time
}

// PrepareCompletion records the attempt and work result together, releasing
// worker capacity independently of unresolved resource effects. Operation state
// and proof must join this change before it is committed.
func PrepareCompletion(request Completion) (Change, error) {
	return prepareCompletion(request, nil)
}

// PrepareCompletionWithRecovery atomically records a forward failure and queues
// its first captured cleanup. Failed recovery itself never uses this path; a new
// recovery after that requires explicit operation-owned Retry admission.
func PrepareCompletionWithRecovery(request Completion, deadline time.Time) (Change, error) {
	if request.Attempt.Attempt.Direction != workexecution.Forward || request.Result.EffectsSettled ||
		request.Result.Outcome == workexecution.Succeeded {
		return Change{}, invalid("automatic cleanup requires an unresolved forward result")
	}
	decision := workexecution.RetryDecision{Direction: workexecution.Recovery, Result: request.Result.Evidence,
		NotBefore: request.At, Deadline: deadline}
	return prepareCompletion(request, &decision)
}

func prepareCompletion(request Completion, recovery *workexecution.RetryDecision) (Change, error) {
	if request.Item.Revision <= 0 || request.Attempt.Revision <= 0 || request.Slot.Revision <= 0 {
		return Change{}, invalid("execution completion requires versioned ownership")
	}
	item, attempt, err := workexecution.Finish(request.Item.Item, request.Attempt.Attempt, request.Result, request.At)
	if err != nil {
		return Change{}, err
	}
	if recovery != nil {
		item, err = workexecution.Retry(item, attempt, *recovery, request.At)
		if err != nil {
			return Change{}, err
		}
	}
	if request.Slot.Record != (Slot{TaskID: item.TaskID, WorkItemID: item.ID, AttemptID: attempt.ID}) {
		return Change{}, invalid("execution completion does not own its worker slot")
	}
	itemBytes, err := EncodeItem(item)
	if err != nil {
		return Change{}, err
	}
	attemptBytes, err := EncodeAttempt(attempt)
	if err != nil {
		return Change{}, err
	}
	slotKey := SlotKey(attempt.Executor, request.Slot.Index)
	change := Change{
		Conditions: []keyvalue.Condition{
			{Key: ItemKey(item.ID), ModRevision: request.Item.Revision},
			{Key: AttemptKey(attempt.ID), ModRevision: request.Attempt.Revision},
			{Key: slotKey, ModRevision: request.Slot.Revision},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: ItemKey(item.ID), Value: itemBytes},
			{Type: keyvalue.MutationPut, Key: AttemptKey(attempt.ID), Value: attemptBytes},
			{Type: keyvalue.MutationDelete, Key: slotKey},
		},
	}
	if recovery != nil {
		reference, err := encode("execution_owner", ClaimOwner{TaskID: item.TaskID, WorkItemID: item.ID})
		if err != nil {
			return Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: QueueKey(item)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: QueueKey(item), Value: reference})
	}
	if len(request.Claims) != len(item.Claims) {
		return Change{}, invalid("execution completion must account for every conflict claim")
	}
	owner := ClaimOwner{TaskID: item.TaskID, WorkItemID: item.ID}
	for index, claim := range item.Claims {
		snapshot := request.Claims[index]
		if snapshot.Claim != claim || snapshot.Revision <= 0 || snapshot.Owner == nil || *snapshot.Owner != owner {
			return Change{}, invalid("execution completion no longer owns its resources")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ClaimKey(claim), ModRevision: snapshot.Revision})
		if item.Phase == workexecution.Settled {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: ClaimKey(claim)})
		}
	}
	if err := completeHostEffect(&change, item, request.HostEffect); err != nil {
		return Change{}, err
	}
	if err := countTransition(&change, request.Task, request.Item.Item, item, request.At); err != nil {
		return Change{}, err
	}
	return change, nil
}
