package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Ready atomically subscribes to the first authenticated Ready report for an
// exact Agent generation. Cancellation closes and unregisters the subscription.
func (r *Registry) Ready(ctx context.Context, agentID string, generation uint64) (<-chan struct{}, error) {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.agents[agentID]
	if state != nil && state.generation == generation && state.online && !state.revoked && state.readyReported {
		return closedSignal(), nil
	}

	key := readyKey{agentID: agentID, generation: generation}
	r.nextSubscription++
	id := r.nextSubscription
	subscription := &readySubscription{signal: make(chan struct{})}
	if r.ready[key] == nil {
		r.ready[key] = make(map[uint64]*readySubscription)
	}
	r.ready[key][id] = subscription
	subscription.stop = context.AfterFunc(ctx, func() {
		r.cancelReadySubscription(key, id, subscription)
	})
	return subscription.signal, nil
}

// TaskTerminal subscribes before an abort is sent, closing the gap between
// demand cancellation and the Agent's durable acknowledgement. The result is
// nil only when the exact live generation reports terminal after persistence.
func (r *Registry) TaskTerminal(
	ctx context.Context,
	agentID string,
	generation uint64,
	taskID string,
	assignmentID string,
) (<-chan error, error) {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "terminal Task id is invalid")
	}
	if ids.Validate(ids.KindAssignment, assignmentID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "terminal Task assignment id is invalid")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.agents[agentID]
	if state == nil || state.generation != generation || !state.online || state.revoked {
		return nil, errs.New(errs.KindStateConflict, "Agent session is not online at the requested generation")
	}
	key := taskTerminalKey{
		agentID: agentID, generation: generation, taskID: taskID, assignmentID: assignmentID,
	}
	r.nextSubscription++
	id := r.nextSubscription
	subscription := &taskTerminalSubscription{result: make(chan error, 1)}
	if r.terminals[key] == nil {
		r.terminals[key] = make(map[uint64]*taskTerminalSubscription)
	}
	r.terminals[key][id] = subscription
	subscription.stop = context.AfterFunc(ctx, func() {
		r.cancelTaskTerminalSubscription(key, id, subscription, ctx.Err())
	})
	return subscription.result, nil
}

func (r *Registry) notifyReadyLocked(key readyKey) {
	subscriptions := r.ready[key]
	delete(r.ready, key)
	for _, subscription := range subscriptions {
		if subscription.stop != nil {
			subscription.stop()
		}
		close(subscription.signal)
	}
}

func (r *Registry) notifyTaskTerminalLocked(key taskTerminalKey) {
	subscriptions := r.terminals[key]
	delete(r.terminals, key)
	for _, subscription := range subscriptions {
		if subscription.stop != nil {
			subscription.stop()
		}
		subscription.result <- nil
		close(subscription.result)
	}
}

func (r *Registry) failTaskTerminalsLocked(agentID string, generation uint64, err error) {
	for key, subscriptions := range r.terminals {
		if key.agentID != agentID || key.generation != generation {
			continue
		}
		delete(r.terminals, key)
		for _, subscription := range subscriptions {
			if subscription.stop != nil {
				subscription.stop()
			}
			subscription.result <- err
			close(subscription.result)
		}
	}
}

func (r *Registry) cancelReadySubscription(key readyKey, id uint64, subscription *readySubscription) {
	r.mu.Lock()
	defer r.mu.Unlock()

	subscriptions := r.ready[key]
	if subscriptions[id] != subscription {
		return
	}
	delete(subscriptions, id)
	if len(subscriptions) == 0 {
		delete(r.ready, key)
	}
	close(subscription.signal)
}

func (r *Registry) cancelTaskTerminalSubscription(
	key taskTerminalKey,
	id uint64,
	subscription *taskTerminalSubscription,
	err error,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	subscriptions := r.terminals[key]
	if subscriptions[id] != subscription {
		return
	}
	delete(subscriptions, id)
	if len(subscriptions) == 0 {
		delete(r.terminals, key)
	}
	if err == nil {
		err = errs.New(errs.KindStateConflict, "Task terminal subscription ended")
	}
	subscription.result <- err
	close(subscription.result)
}
