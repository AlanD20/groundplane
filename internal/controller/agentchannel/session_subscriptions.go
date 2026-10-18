package agentchannel

import (
	"context"
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
