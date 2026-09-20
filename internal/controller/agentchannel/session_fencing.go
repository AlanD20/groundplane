package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StopAssignments prevents the current session from receiving new work.
func (r *Registry) StopAssignments(ctx context.Context, agentID string, generation uint64) error {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return err
	}
	r.mu.Lock()
	lifecycle := r.lifecycleFenceLocked(agentID)
	if generation > lifecycle.quiescedThrough {
		lifecycle.quiescedThrough = generation
	}
	lifecycle.discardPausedThrough(generation)
	state, ok := r.agents[agentID]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	if state.generation > generation {
		r.mu.Unlock()
		return generationConflict()
	}
	state.assignmentsStopped = true
	state.fenceAssignmentSendsLocked()
	r.mu.Unlock()
	return drainAssignmentSend(ctx, state)
}

// FenceThrough removes all assignment and channel authority at or below one
// durable generation bound. A newer registered generation proves the prior
// capability is already fenced and is never stopped, revoked, or canceled.
func (r *Registry) FenceThrough(ctx context.Context, agentID string, generation uint64) error {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return err
	}

	r.mu.Lock()
	lifecycle := r.lifecycleFenceLocked(agentID)
	if generation > lifecycle.quiescedThrough {
		lifecycle.quiescedThrough = generation
	}
	if generation > lifecycle.revokedThrough {
		lifecycle.revokedThrough = generation
	}
	lifecycle.discardPausedThrough(generation)
	state := r.agents[agentID]
	if state == nil || state.generation > generation {
		r.mu.Unlock()
		return nil
	}
	state.assignmentsStopped = true
	state.fenceAssignmentSendsLocked()
	if !state.revoked {
		state.revoked = true
		state.cancel()
	}
	online := state.online
	offline := state.offline
	r.mu.Unlock()
	if err := drainAssignmentSend(ctx, state); err != nil {
		return err
	}
	if !online {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-offline:
		return nil
	}
}

// Revoke fences one exact in-memory generation after its credential has been
// durably revoked by the lifecycle repository.
func (r *Registry) Revoke(ctx context.Context, agentID string, generation uint64) error {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return err
	}

	r.mu.Lock()
	lifecycle := r.lifecycleFenceLocked(agentID)
	if generation > lifecycle.quiescedThrough {
		lifecycle.quiescedThrough = generation
	}
	if generation > lifecycle.revokedThrough {
		lifecycle.revokedThrough = generation
	}
	lifecycle.discardPausedThrough(generation)
	state, ok := r.agents[agentID]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	if state.generation > generation {
		r.mu.Unlock()
		return generationConflict()
	}
	state.assignmentsStopped = true
	state.fenceAssignmentSendsLocked()
	state.revoked = true
	state.cancel()
	r.mu.Unlock()
	return drainAssignmentSend(ctx, state)
}

// WaitOffline waits until one exact registered generation has closed.
func (r *Registry) WaitOffline(ctx context.Context, agentID string, generation uint64) error {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return err
	}
	r.mu.Lock()
	state, ok := r.agents[agentID]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	if state.generation > generation {
		r.mu.Unlock()
		return generationConflict()
	}
	if !state.online {
		r.mu.Unlock()
		return nil
	}
	offline := state.offline
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-offline:
		return nil
	}
}

func (r *Registry) lifecycleFenceLocked(agentID string) *lifecycleFence {
	lifecycle := r.lifecycle[agentID]
	if lifecycle == nil {
		lifecycle = &lifecycleFence{}
		r.lifecycle[agentID] = lifecycle
	}
	return lifecycle
}

func validateLifecycleTarget(ctx context.Context, agentID string, generation uint64) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "agent session context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if agentID == "" {
		return errs.New(errs.KindValidationFailed, "agent id is required")
	}
	if generation == 0 {
		return errs.New(errs.KindValidationFailed, "agent generation is required")
	}
	return nil
}

func generationConflict() error {
	return errs.New(errs.KindStateConflict, "agent session generation does not match")
}

func closedSignal() <-chan struct{} {
	signal := make(chan struct{})
	close(signal)
	return signal
}
