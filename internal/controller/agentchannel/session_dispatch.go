package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
	"unicode/utf8"
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
	current.version = ""
	return true
}

// RecordReady records the most recent reported free capacity and binary
// version for this session.
func (s *Session) RecordReady(at time.Time, capacity int32, version string) error {
	if err := validateReady(capacity, version); err != nil {
		return err
	}

	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online || current.revoked {
		return errs.New(errs.KindStateConflict, "agent session is fenced")
	}
	current.lastReady = at
	current.readyReported = true
	current.capacity = capacity
	current.version = version
	s.registry.notifyReadyLocked(readyKey{agentID: s.agentID, generation: current.generation})
	return nil
}

// RecordTaskTerminal publishes completion only after the caller has committed
// the durable Task acknowledgement. A fenced session cannot satisfy a waiter
// belonging to its replacement.
func (s *Session) RecordTaskTerminal(taskID string, assignmentID string) error {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindAssignment, assignmentID) != nil {
		return errs.New(errs.KindValidationFailed, "terminal Task id is invalid")
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online || current.revoked {
		return errs.New(errs.KindStateConflict, "agent session is fenced")
	}
	s.registry.notifyTaskTerminalLocked(taskTerminalKey{
		agentID: s.agentID, generation: current.generation, taskID: taskID, assignmentID: assignmentID,
	})
	return nil
}

func (s *Session) taskAborts() <-chan taskAbortCommand {
	return s.state.aborts
}

func (s *Session) logMessages() <-chan logCommand {
	return s.state.logCommands
}

// AbortTask synchronously hands one demand-cancellation command to the sole
// send loop for the exact authenticated Agent generation. The unbuffered
// rendezvous prevents a disconnected or stalled Agent from accumulating an
// in-memory command queue.
func (r *Registry) AbortTask(
	ctx context.Context,
	agentID string,
	generation uint64,
	taskID string,
	assignmentID string,
	reason string,
) error {
	if err := validateLifecycleTarget(ctx, agentID, generation); err != nil {
		return err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return errs.New(errs.KindValidationFailed, "Task abort id is invalid")
	}
	if ids.Validate(ids.KindAssignment, assignmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Task abort assignment id is invalid")
	}
	if reason == "" || len(reason) > maximumTaskAbortReasonBytes || !utf8.ValidString(reason) {
		return errs.New(errs.KindValidationFailed, "Task abort reason is invalid")
	}

	r.mu.Lock()
	state := r.agents[agentID]
	if state == nil || state.generation != generation || !state.online || state.revoked {
		r.mu.Unlock()
		return errs.New(errs.KindStateConflict, "Agent session is not online at the requested generation")
	}
	command := taskAbortCommand{
		taskID: taskID, assignmentID: assignmentID, reason: reason, result: make(chan error, 1),
	}
	aborts := state.aborts
	done := state.done
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return errs.New(errs.KindStateConflict, "Agent session ended before Task abort delivery")
	case aborts <- command:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return errs.New(errs.KindStateConflict, "Agent session ended during Task abort delivery")
	case err := <-command.result:
		return err
	}
}
