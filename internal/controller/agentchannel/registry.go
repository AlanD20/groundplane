package agentchannel

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Snapshot is the non-secret, point-in-time state of an Agent session.
type Snapshot struct {
	Generation         uint64
	Online             bool
	AssignmentsStopped bool
	Revoked            bool
	LastReady          time.Time
	Capacity           int32
}

// RevocationHook durably revokes the credential for an Agent generation.
// Implementations should return the canonical errs.Error type.
type RevocationHook func(context.Context, string, uint64) error

// Registry owns the single active session for each Agent.
type Registry struct {
	mu     sync.Mutex
	next   uint64
	agents map[string]*sessionState
}

type sessionState struct {
	generation         uint64
	fence              uint64
	online             bool
	assignmentsStopped bool
	revoked            bool
	revoking           bool
	lastReady          time.Time
	capacity           int32
	cancel             context.CancelFunc
	offline            chan struct{}
	offlineOnce        sync.Once
}

// Session is a fenced capability for mutating one active Agent session.
type Session struct {
	registry *Registry
	agentID  string
	state    *sessionState
	ctx      context.Context
	close    sync.Once
}

// NewRegistry returns an empty Agent session registry.
func NewRegistry() *Registry {
	return &Registry{agents: make(map[string]*sessionState)}
}

// Open registers an authenticated session. An equal or newer generation
// replaces and fences the previous connection; an older generation is rejected.
func (r *Registry) Open(parent context.Context, agentID string, generation uint64) (*Session, error) {
	if agentID == "" {
		return nil, errs.New(errs.KindValidationFailed, "agent id is required")
	}
	if parent == nil {
		return nil, errs.New(errs.KindInternal, "agent session context is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.agents[agentID]
	if current != nil {
		if current.revoked || current.revoking {
			return nil, errs.New(errs.KindStateConflict, "agent session is revoked")
		}
		if generation < current.generation {
			return nil, errs.New(errs.KindStateConflict, "agent session generation is stale")
		}
		if current.online {
			current.cancel()
		}
	}

	r.next++
	ctx, cancel := context.WithCancel(parent)
	state := &sessionState{
		generation: generation,
		fence:      r.next,
		online:     true,
		cancel:     cancel,
		offline:    make(chan struct{}),
	}
	r.agents[agentID] = state

	return &Session{
		registry: r,
		agentID:  agentID,
		state:    state,
		ctx:      ctx,
	}, nil
}

// Done closes when the session is replaced, revoked, or its parent is canceled.
func (s *Session) Done() <-chan struct{} {
	return s.ctx.Done()
}

// RecordReady records the most recent reported free capacity for this session.
func (s *Session) RecordReady(at time.Time, capacity int32) error {
	if capacity < 0 {
		return errs.New(errs.KindValidationFailed, "agent Ready capacity must be non-negative")
	}

	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online || current.revoked {
		return errs.New(errs.KindStateConflict, "agent session is fenced")
	}
	current.lastReady = at
	current.capacity = capacity
	return nil
}

// AssignmentsAllowed reports whether this session may receive new work.
func (s *Session) AssignmentsAllowed() bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	return current == s.state && current.fence == s.state.fence && current.online &&
		!current.assignmentsStopped && !current.revoked
}

// Close marks this session offline if it still owns the active fence.
func (s *Session) Close() {
	s.close.Do(func() {
		s.state.cancel()

		s.registry.mu.Lock()
		if s.registry.agents[s.agentID] == s.state {
			s.state.online = false
		}
		s.registry.mu.Unlock()

		s.state.offlineOnce.Do(func() { close(s.state.offline) })
	})
}

// Snapshot returns non-secret state for the latest known Agent generation.
func (r *Registry) Snapshot(agentID string) (Snapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.agents[agentID]
	if !ok {
		return Snapshot{}, false
	}
	return Snapshot{
		Generation:         state.generation,
		Online:             state.online,
		AssignmentsStopped: state.assignmentsStopped,
		Revoked:            state.revoked,
		LastReady:          state.lastReady,
		Capacity:           state.capacity,
	}, true
}

// StopAssignments prevents the current session from receiving new work.
func (r *Registry) StopAssignments(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.agents[agentID]
	if !ok {
		return errs.New(errs.KindAgentNotFound, "agent session was not found")
	}
	state.assignmentsStopped = true
	return nil
}

// Revoke runs the durable revocation hook and then fences the active session.
// Callers stop assignments and abort active tasks before invoking this method.
func (r *Registry) Revoke(ctx context.Context, agentID string, hook RevocationHook) error {
	if hook == nil {
		return errs.New(errs.KindInternal, "agent revocation hook is required")
	}

	r.mu.Lock()
	state, ok := r.agents[agentID]
	if !ok {
		r.mu.Unlock()
		return errs.New(errs.KindAgentNotFound, "agent session was not found")
	}
	if state.revoked {
		r.mu.Unlock()
		return nil
	}
	if state.revoking {
		r.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent revocation is already in progress")
	}
	state.assignmentsStopped = true
	state.revoking = true
	generation := state.generation
	r.mu.Unlock()

	if err := hook(ctx, agentID, generation); err != nil {
		r.mu.Lock()
		state.revoking = false
		r.mu.Unlock()

		var domainErr *errs.Error
		if errors.As(err, &domainErr) {
			return domainErr
		}
		return errs.Wrap(errs.KindInternal, err)
	}

	r.mu.Lock()
	state.revoking = false
	state.revoked = true
	state.cancel()
	r.mu.Unlock()
	return nil
}

// WaitOffline waits until the latest registered session has closed.
func (r *Registry) WaitOffline(ctx context.Context, agentID string) error {
	r.mu.Lock()
	state, ok := r.agents[agentID]
	if !ok || !state.online {
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
