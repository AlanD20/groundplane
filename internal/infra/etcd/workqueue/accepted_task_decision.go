package workqueue

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareAcceptedTaskDecision preserves an existing operation's atomic,
// metadata-only Controller decision. The resource and immutable result writes
// must join this change. Nothing is queued, assigned or represented as a host
// effect merely to record that the decision succeeded.
func (repository *Repository) PrepareAcceptedTaskDecision(ctx context.Context, snapshot TaskSnapshot,
	prepared workexecution.Task, item workexecution.Item, evidence workexecution.Input, at time.Time,
) (Change, error) {
	if prepared.ExpectedItems != 1 || item.Placement.Executor != workexecution.Controller ||
		item.Phase != workexecution.Waiting || item.Input != nil || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		len(item.Dependencies) != 0 || !at.Before(item.Deadline) {
		return Change{}, invalid(
			"atomic accepted decision requires one unstarted Controller item and exact result evidence",
		)
	}
	base, err := repository.PrepareAcceptedTaskPromotion(
		ctx,
		snapshot,
		prepared,
		[]workexecution.Item{item},
		nil,
		false,
		at,
	)
	if err != nil {
		return Change{}, err
	}
	defer keyvalue.ClearMutationValues(base.Mutations)
	settled, err := workexecution.SettleUnstarted(
		item,
		workexecution.Completion{Outcome: workexecution.Succeeded, EffectsSettled: true, Evidence: evidence},
		at,
	)
	if err != nil {
		return Change{}, err
	}
	next, err := workexecution.CountPublication(prepared, []workexecution.Item{item}, at)
	if err != nil {
		return Change{}, err
	}
	next.PublicationComplete = true
	next, err = workexecution.CountTransition(next, item, settled, at)
	if err != nil {
		return Change{}, err
	}
	change, err := taskChange(snapshot, next)
	if err != nil {
		return Change{}, err
	}
	itemBytes, err := EncodeItem(settled)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, base.Conditions...)
	for _, claim := range item.Claims {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ClaimKey(claim)})
	}
	if prepared.Scope.EnvironmentID != "" {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: TaskReservationKey(workexecution.Claim{ScopeID: prepared.Scope.EnvironmentID})},
		)
	}
	for _, mutation := range base.Mutations {
		if mutation.Key == ItemKey(item.ID) || mutation.Key == inputQueueKey(item.ID) {
			continue
		}
		replaced := false
		for _, current := range change.Mutations {
			if current.Key == mutation.Key {
				replaced = true
				break
			}
		}
		if replaced {
			continue
		}
		mutation.Value = append([]byte(nil), mutation.Value...)
		change.Mutations = append(change.Mutations, mutation)
	}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ItemKey(item.ID), Value: itemBytes},
	)
	return change, nil
}
