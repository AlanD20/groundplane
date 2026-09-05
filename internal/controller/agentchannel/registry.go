package agentchannel

import (
	"context"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
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

func drainAssignmentSend(ctx context.Context, state *sessionState) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-state.sendPermit:
	}
	state.sendPermit <- struct{}{}
	return ctx.Err()
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
		sendPermit := make(chan struct{}, 1)
		sendPermit <- struct{}{}
		state := &sessionState{
			generation:         generation,
			fence:              r.nextFence,
			online:             true,
			assignmentsStopped: lifecycle != nil && generation <= lifecycle.quiescedThrough,
			cancel:             cancel,
			dispatchWake:       make(chan struct{}, 1),
			sendPermit:         sendPermit,
			sendFence:          make(chan struct{}),
			done:               ctx.Done(),
			aborts:             make(chan taskAbortCommand),
			logCommands:        make(chan logCommand),
			imageCommands:      make(chan *imageCommand),
			imageCounter:       ids.NewImageCorrelationCounter(),
			offline:            make(chan struct{}),
		}
		if state.assignmentsStopped {
			state.fenceAssignmentSendsLocked()
		}
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

// sendAssignment admits one complete assignment payload only while the
// session owns its current generation. Fencing takes the registry lock before
// this gate, so once fencing completes no new assignment send can begin.
func (s *Session) sendAssignment(send func() error) (bool, error) {
	select {
	case <-s.ctx.Done():
		return false, nil
	case <-s.state.sendFence:
		return false, nil
	case <-s.state.sendPermit:
	}
	defer func() { s.state.sendPermit <- struct{}{} }()

	s.registry.mu.Lock()
	current := s.registry.agents[s.agentID]
	if current != s.state || current.fence != s.state.fence || !current.online ||
		current.assignmentsStopped || current.revoked {
		s.registry.mu.Unlock()
		return false, nil
	}
	s.registry.mu.Unlock()

	return true, send()
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

// AssignmentsAllowed reports whether this session may receive new work.
func (s *Session) AssignmentsAllowed() bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()

	current := s.registry.agents[s.agentID]
	return current == s.state && current.fence == s.state.fence && current.online &&
		!current.assignmentsStopped && !current.revoked
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
		AssignmentsStopped: state.assignmentsStopped,
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
