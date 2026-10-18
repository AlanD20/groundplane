package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

// PrepareSettledDecision publishes an already-applied Controller decision in
// the same transaction as its resource writes and immutable result evidence.
// Nothing is dispatched and no execution attempt or lease is manufactured.
func PrepareSettledDecision(task workexecution.Task, item workexecution.Item, evidence workexecution.Input,
	marker idempotency.IdempotencyMarker, at time.Time,
) (Change, error) {
	if item.Placement.Executor != workexecution.Controller || item.Phase != workexecution.Waiting ||
		item.Input != nil || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput || len(item.Dependencies) != 0 ||
		at.IsZero() || at.Location() != time.UTC || at.Before(task.CreatedAt) || !at.Before(task.Deadline) {
		return Change{}, invalid("atomic decision requires unstarted Controller work")
	}
	change, err := PrepareSingleTask(task, item, marker)
	if err != nil {
		return Change{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	nextItem, err := workexecution.SettleUnstarted(item, workexecution.Completion{
		Outcome: workexecution.Succeeded, EffectsSettled: true, Evidence: evidence,
	}, at)
	if err != nil {
		return Change{}, err
	}
	nextTask, err := workexecution.CountPublication(task, []workexecution.Item{item}, task.CreatedAt)
	if err != nil {
		return Change{}, err
	}
	nextTask.PublicationComplete = true
	nextTask, err = workexecution.CountTransition(nextTask, item, nextItem, at)
	if err != nil {
		return Change{}, err
	}
	taskBytes, err := EncodeTask(nextTask)
	if err != nil {
		return Change{}, err
	}
	itemBytes, err := EncodeItem(nextItem)
	if err != nil {
		clear(taskBytes)
		return Change{}, err
	}
	result := Change{Conditions: append([]keyvalue.Condition(nil), change.Conditions...)}
	for _, claim := range item.Claims {
		result.Conditions = append(result.Conditions, keyvalue.Condition{Key: ClaimKey(claim)})
	}
	if task.Scope.EnvironmentID != "" {
		result.Conditions = append(result.Conditions, keyvalue.Condition{Key: TaskReservationKey(workexecution.Claim{ScopeID: task.Scope.EnvironmentID})})
	}
	for _, mutation := range change.Mutations {
		switch mutation.Key {
		case TaskKey(task.ID), ItemKey(item.ID), inputQueueKey(item.ID):
			continue
		}
		mutation.Value = append([]byte(nil), mutation.Value...)
		result.Mutations = append(result.Mutations, mutation)
	}
	result.Mutations = append(result.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: TaskKey(task.ID), Value: taskBytes},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ItemKey(item.ID), Value: itemBytes},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: taskFinishKey(task.ID), Value: []byte(task.ID)})
	return result, nil
}
