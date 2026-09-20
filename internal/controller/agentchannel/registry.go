package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	maximumTaskAbortReasonBytes = 128
	maximumAgentVersionBytes    = 128
)

// Snapshot is the non-secret, point-in-time state of an Agent session.
type Snapshot struct {
	Generation         uint64
	Online             bool
	AssignmentsStopped bool
	Revoked            bool
	LastReady          time.Time
	Capacity           int32
	Version            string
}

// Registry owns the single active session for each Agent.
type Registry struct {
	mu               sync.Mutex
	nextFence        uint64
	nextSubscription uint64
	agents           map[string]*sessionState
	lifecycle        map[string]*lifecycleFence
	ready            map[readyKey]map[uint64]*readySubscription
	terminals        map[taskTerminalKey]map[uint64]*taskTerminalSubscription
	logs             map[string]*LogSubscription
}

type lifecycleFence struct {
	quiescedThrough uint64
	revokedThrough  uint64
	pauses          map[*assignmentPause]struct{}
	preparations    map[*assignmentPreparation]struct{}
}

type taskAbortCommand struct {
	taskID       string
	assignmentID string
	reason       string
	result       chan error
}

type logCommand struct {
	message *agentpb.ControllerMessage
	result  chan error
}

type readyKey struct {
	agentID    string
	generation uint64
}

type readySubscription struct {
	signal chan struct{}
	stop   func() bool
}

type taskTerminalKey struct {
	agentID      string
	generation   uint64
	taskID       string
	assignmentID string
}

type taskTerminalSubscription struct {
	result chan error
	stop   func() bool
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
	return &Registry{
		agents:    make(map[string]*sessionState),
		lifecycle: make(map[string]*lifecycleFence),
		ready:     make(map[readyKey]map[uint64]*readySubscription),
		terminals: make(map[taskTerminalKey]map[uint64]*taskTerminalSubscription),
		logs:      make(map[string]*LogSubscription),
	}
}

// fenceAssignmentSendsLocked closes assignment admission for one session.
// The caller holds Registry.mu.
func (state *sessionState) fenceAssignmentSendsLocked() {
	if state.sendFenced {
		return
	}
	state.sendFenced = true
	close(state.sendFence)
}

// WakeTaskDispatch notifies every connected Agent that new work may be
// available. The signal is coalescing; the queue and the last Ready capacity
// remain authoritative when the session handles it.
func (r *Registry) WakeTaskDispatch() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, state := range r.agents {
		if !state.online || state.revoked {
			continue
		}
		select {
		case state.dispatchWake <- struct{}{}:
		default:
		}
	}
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
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, errs.New(errs.KindValidationFailed, "agent generation is required")
	}

	for {
		r.mu.Lock()
		if err := parent.Err(); err != nil {
			r.mu.Unlock()
			return nil, err
		}
		lifecycle := r.lifecycle[agentID]
		if lifecycle != nil && generation <= lifecycle.revokedThrough {
			r.mu.Unlock()
			return nil, errs.New(errs.KindStateConflict, "agent session generation is revoked")
		}
		current := r.agents[agentID]
		if current != nil {
			if generation < current.generation {
				r.mu.Unlock()
				return nil, errs.New(errs.KindStateConflict, "agent session generation is stale")
			}
			if generation == current.generation && current.revoked {
				r.mu.Unlock()
				return nil, errs.New(errs.KindStateConflict, "agent session generation is revoked")
			}
			current.assignmentsStopped = true
			current.fenceAssignmentSendsLocked()
			if current.online {
				current.cancel()
			}
		}
		r.mu.Unlock()

		if current != nil {
			if err := drainAssignmentSend(parent, current); err != nil {
				return nil, err
			}
		}

		r.mu.Lock()
		if err := parent.Err(); err != nil {
			r.mu.Unlock()
			return nil, err
		}
		if r.agents[agentID] != current {
			r.mu.Unlock()
			continue
		}
		lifecycle = r.lifecycle[agentID]
		if lifecycle != nil && generation <= lifecycle.revokedThrough {
			r.mu.Unlock()
			return nil, errs.New(errs.KindStateConflict, "agent session generation is revoked")
		}

		r.nextFence++
		ctx, cancel := context.WithCancel(parent)
		state := newSessionState(
			ctx,
			cancel,
			generation,
			r.nextFence,
			lifecycle != nil && generation <= lifecycle.quiescedThrough,
		)
		r.agents[agentID] = state
		r.mu.Unlock()

		return &Session{
			registry: r,
			agentID:  agentID,
			state:    state,
			ctx:      ctx,
		}, nil
	}
}

// Close marks this session offline if it still owns the active fence.
func (s *Session) Close() {
	s.close.Do(func() {
		s.state.cancel()

		s.registry.mu.Lock()
		if s.registry.agents[s.agentID] == s.state {
			s.state.assignmentsStopped = true
			s.state.fenceAssignmentSendsLocked()
			s.state.online = false
			s.registry.failTaskTerminalsLocked(
				s.agentID,
				s.state.generation,
				errs.New(errs.KindStateConflict, "Agent session ended before Task terminal acknowledgement"),
			)
			s.registry.failLogsLocked(s.state, errs.New(errs.KindStorageUnavailable, "Agent log session ended"))
		} else {
			s.registry.failTaskTerminalsLocked(
				s.agentID,
				s.state.generation,
				errs.New(errs.KindStateConflict, "Agent session ended before Task terminal acknowledgement"),
			)
			s.registry.failLogsLocked(s.state, errs.New(errs.KindStorageUnavailable, "Agent log session ended"))
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
		AssignmentsStopped: state.assignmentsStopped || r.assignmentsPausedLocked(agentID, state.generation),
		Revoked:            state.revoked,
		LastReady:          state.lastReady,
		Capacity:           state.capacity,
		Version:            state.version,
	}, true
}

func validateReady(capacity int32, version string) error {
	if capacity < 0 {
		return errs.New(errs.KindValidationFailed, "agent Ready capacity must be non-negative")
	}
	if version == "" || len(version) > maximumAgentVersionBytes || !utf8.ValidString(version) {
		return errs.New(errs.KindValidationFailed, "agent Ready version is invalid")
	}
	return nil
}
