package agentchannel

import (
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// Done closes when the session is replaced, revoked, or its parent is canceled.
func (s *Session) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Session) taskDispatchWake() <-chan struct{} {
	return s.state.dispatchWake
}

func (s *Session) dispatchCapacity() (int32, bool) {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online || current.revoked ||
		!current.readyReported {
		return 0, false
	}
	return current.capacity, true
}

func (s *Session) recordDispatchCapacity(capacity int32) bool {
	if capacity < 0 {
		return false
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online || current.revoked ||
		!current.readyReported {
		return false
	}
	current.capacity = capacity
	return true
}

func (s *Session) invalidateReady() bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online || current.revoked {
		return false
	}
	current.readyReported = false
	current.lastReady = time.Time{}
	current.capacity = 0
	return true
}

// RecordReady records the most recent reported free capacity and binary
// version for this session.
func (s *Session) RecordReady(at time.Time, capacity int32, version string, source buildversion.BuildProvenance) error {
	if err := validateReady(capacity, version); err != nil {
		return err
	}
	if err := source.Validate(); err != nil {
		return err
	}

	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online || current.revoked {
		return errs.New(errs.KindStateConflict, "agent session is fenced")
	}
	if current.version != "" && (current.version != version || current.source != source) {
		return errs.New(errs.KindStateConflict, "agent build identity changed within one session")
	}
	current.lastReady = at
	current.readyReported = true
	current.capacity = capacity
	current.version = version
	current.source = source
	s.registry.notifyReadyLocked(readyKey{agentID: s.agentID, generation: current.generation})
	return nil
}

func (s *Session) logMessages() <-chan logCommand {
	return s.state.logCommands
}
