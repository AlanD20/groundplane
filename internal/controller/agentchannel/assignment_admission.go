package agentchannel

import (
	"context"
	"sync"
)

// assignmentPause is an operation-owned hold, distinct from irreversible
// generation revocation or deletion. All access is under Registry.mu.
type assignmentPause struct {
	generation uint64
}

type assignmentPreparation struct {
	generation uint64
	done       chan struct{}
}

// PauseAssignments closes admission and drains already admitted dispatch/send
// work. The returned idempotent release drops only this operation's hold; it
// never resets a lifecycle fence or cancels active Agent work. Failed entry
// releases its hold, including when cancellation interrupts the bounded drain.
func (r *Registry) PauseAssignments(ctx context.Context, agentID string, generation uint64) (func(), error) {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return nil, err
	}
	r.mu.Lock()
	lifecycle := r.lifecycleFenceLocked(agentID)
	state := r.agents[agentID]
	if generation <= lifecycle.revokedThrough || state != nil && state.generation > generation {
		r.mu.Unlock()
		return nil, generationConflict()
	}
	pause := &assignmentPause{generation: generation}
	if lifecycle.pauses == nil {
		lifecycle.pauses = make(map[*assignmentPause]struct{})
	}
	lifecycle.pauses[pause] = struct{}{}
	var pending []<-chan struct{}
	for preparation := range lifecycle.preparations {
		if preparation.generation <= generation {
			pending = append(pending, preparation.done)
		}
	}
	r.mu.Unlock()

	var once sync.Once
	resume := func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			delete(lifecycle.pauses, pause)
			current := r.agents[agentID]
			if current == nil || current.generation > generation || !current.online ||
				current.assignmentsStopped || current.revoked || r.assignmentsPausedLocked(agentID, current.generation) {
				return
			}
			select {
			case current.dispatchWake <- struct{}{}:
			default:
			}
		})
	}
	for _, done := range pending {
		select {
		case <-ctx.Done():
			resume()
			return nil, ctx.Err()
		case <-done:
		}
	}
	if state != nil {
		if err := drainAssignmentSend(ctx, state); err != nil {
			resume()
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		resume()
		return nil, err
	}
	return resume, nil
}

func (r *Registry) assignmentsPausedLocked(agentID string, generation uint64) bool {
	lifecycle := r.lifecycle[agentID]
	if lifecycle != nil {
		for pause := range lifecycle.pauses {
			if generation <= pause.generation {
				return true
			}
		}
	}
	return false
}

func (fence *lifecycleFence) discardPausedThrough(generation uint64) {
	for pause := range fence.pauses {
		if pause.generation <= generation {
			delete(fence.pauses, pause)
		}
	}
}

// beginDispatch includes durable claim preparation in the admission drain, so
// an idle check cannot race a claim that has not yet reached the send gate.
func (s *Session) beginDispatch() (func(), bool) {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	if !s.assignmentsAllowedLocked() {
		return nil, false
	}
	lifecycle := s.registry.lifecycleFenceLocked(s.agentID)
	preparation := &assignmentPreparation{generation: s.state.generation, done: make(chan struct{})}
	if lifecycle.preparations == nil {
		lifecycle.preparations = make(map[*assignmentPreparation]struct{})
	}
	lifecycle.preparations[preparation] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			s.registry.mu.Lock()
			defer s.registry.mu.Unlock()
			delete(lifecycle.preparations, preparation)
			close(preparation.done)
		})
	}, true
}

func drainAssignmentSend(ctx context.Context, state *sessionState) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-state.sendPermit:
	}
	state.sendPermit <- struct{}{}
	return ctx.Err()
}

// sendAssignment admits a complete payload only while this session owns its
// generation and no irreversible fence or reversible pause closes admission.
func (s *Session) sendAssignment(send func() error) (bool, error) {
	select {
	case <-s.ctx.Done():
		return false, nil
	case <-s.state.sendFence:
		return false, nil
	case <-s.state.sendPermit:
	}
	defer func() { s.state.sendPermit <- struct{}{} }()
	if !s.AssignmentsAllowed() {
		return false, nil
	}
	return true, send()
}

// AssignmentsAllowed reports current admission, including reversible holds.
func (s *Session) AssignmentsAllowed() bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	return s.assignmentsAllowedLocked()
}

func (s *Session) assignmentsAllowedLocked() bool {
	current := s.registry.agents[s.agentID]
	return current == s.state && current.fence == s.state.fence && current.online &&
		!current.assignmentsStopped && !current.revoked &&
		!s.registry.assignmentsPausedLocked(s.agentID, current.generation)
}
