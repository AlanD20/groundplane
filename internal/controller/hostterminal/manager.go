// Package hostterminal owns transient, content-free metadata and the bounded
// byte transport for interactive host and container processes. Sessions are deliberately not
// Tasks and are never written to the durable store or application logs.
package hostterminal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"os"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaxSessions        = 4
	maxReplayBytes     = 1 << 20
	outputReadBytes    = 16 << 10
	attachmentCapacity = 64
	maxReplayKeys      = 128
)

type shellLauncher interface {
	StartHostShell(context.Context, string, runner.HostShellSize) (*runner.HostPTY, error)
}

type outputFrame struct {
	Sequence uint64
	Data     []byte
}

type EventKind uint8

const (
	EventOutput EventKind = iota
	EventActivity
	EventTerminated
)

type Event struct {
	Kind     EventKind
	Sequence uint64
	Data     []byte
	Session  apiTypes.TerminalSession
}

type Attachment struct {
	Replay    []Event
	Events    <-chan Event
	Truncated bool
	detach    func()
}

func (a Attachment) Detach() {
	if a.detach != nil {
		a.detach()
	}
}

type Manager struct {
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	launcher    shellLauncher
	inactivity  time.Duration
	hostname    string
	closed      bool
	byID        map[string]*session
	replays     map[string]*terminalReplay
	opening     int
	replayOrder []string
}

type terminalReplay struct {
	owner       string
	fingerprint [32]byte
	done        chan struct{}
	session     *session
	err         error
}

type Target struct {
	Kind             string
	ServiceID        string
	ContainerID      string
	ContainerName    string
	User             string
	Command          []string
	WorkingDirectory string
	Columns          uint16
	Rows             uint16
}

type hostProcess struct{ *runner.HostPTY }

func (p hostProcess) Result() terminalsession.Exit {
	code := 0
	if p.Wait() != nil {
		code = -1
	}
	return terminalsession.Exit{Reason: "command_exited", Code: code}
}

func (p hostProcess) Resize(size terminalsession.Size) error {
	return p.HostPTY.Resize(runner.HostShellSize{Columns: size.Columns, Rows: size.Rows})
}

type session struct {
	manager *Manager
	owner   string
	id      string
	pty     terminalsession.Process
	target  Target
	cancel  context.CancelFunc

	mu                 sync.Mutex
	expiresAt          time.Time
	timer              *time.Timer
	frames             []outputFrame
	frameBytes         int
	sequence           uint64
	closed             bool
	terminationDone    chan struct{}
	exitCode           *int
	reason             string
	attachment         chan Event
	lastActivityNotice time.Time
}

func New(launcher shellLauncher, inactivity time.Duration) (*Manager, error) {
	if launcher == nil {
		return nil, errs.New(errs.KindInternal, "host terminal launcher is required")
	}
	if inactivity < time.Minute || inactivity > 8*time.Hour {
		return nil, errs.New(errs.KindValidationFailed, "host terminal inactivity must be between 1m and 8h")
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return nil, errs.Wrap(errs.KindInternal, errors.New("host terminal hostname is unavailable"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		ctx: ctx, cancel: cancel, launcher: launcher, inactivity: inactivity,
		hostname: hostname, byID: make(map[string]*session),
		replays: make(map[string]*terminalReplay),
	}, nil
}

func (m *Manager) Open(
	ctx context.Context, owner, idempotencyKey string, columns, rows uint16,
) (apiTypes.TerminalSession, string, error) {
	target := Target{Kind: "host", User: "root", Columns: columns, Rows: rows}
	return m.OpenProcess(
		ctx,
		owner,
		idempotencyKey,
		target,
		func(ctx context.Context, id string) (terminalsession.Process, error) {
			process, err := m.launcher.StartHostShell(ctx, id, runner.HostShellSize{Columns: columns, Rows: rows})
			if err != nil {
				return nil, err
			}
			return hostProcess{process}, nil
		},
	)
}

// OpenProcess reserves shared capacity before launching without holding the
// manager lock. Concurrent retries wait for the same captured startup result.
func (m *Manager) OpenProcess(
	ctx context.Context, owner, key string, target Target,
	start func(context.Context, string) (terminalsession.Process, error),
) (apiTypes.TerminalSession, string, error) {
	if err := ctx.Err(); err != nil {
		return apiTypes.TerminalSession{}, "", err
	}
	if key == "" || start == nil {
		return apiTypes.TerminalSession{}, "", errs.New(
			errs.KindMalformedRequest,
			"terminal request identity is unavailable",
		)
	}
	if err := (terminalsession.Size{Columns: target.Columns, Rows: target.Rows}).Validate(); err != nil {
		return apiTypes.TerminalSession{}, "", err
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return apiTypes.TerminalSession{}, "", errs.Wrap(errs.KindInternal, err)
	}
	fingerprint := sha256.Sum256(encoded)
	m.mu.Lock()
	if replay, ok := m.replays[key]; ok {
		m.mu.Unlock()
		if replay.fingerprint != fingerprint || owner != "" && replay.owner != owner {
			return apiTypes.TerminalSession{}, "", errs.New(
				errs.KindIdempotencyMismatch,
				"Idempotency-Key was already used for a different terminal request",
			)
		}
		select {
		case <-ctx.Done():
			return apiTypes.TerminalSession{}, "", ctx.Err()
		case <-replay.done:
		}
		if replay.err != nil {
			return apiTypes.TerminalSession{}, "", replay.err
		}
		return replay.session.projection(true), replay.owner, nil
	}
	if m.closed || !m.ensureReplayCapacityLocked() || len(m.byID)+m.opening >= MaxSessions {
		m.mu.Unlock()
		return apiTypes.TerminalSession{}, "", errs.New(
			errs.KindStateConflict,
			"terminal session or request limit reached",
		)
	}
	if owner == "" {
		owner = ids.NewULID()
	}
	replay := &terminalReplay{owner: owner, fingerprint: fingerprint, done: make(chan struct{})}
	m.recordReplayLocked(key, replay)
	m.opening++
	m.mu.Unlock()

	id := ids.New(ids.KindTerminal)
	processContext, cancel := context.WithCancel(m.ctx)
	stopRequestCancellation := context.AfterFunc(ctx, cancel)
	process, err := start(processContext, id)
	requestStopped := stopRequestCancellation()
	if err == nil && (!requestStopped || ctx.Err() != nil) {
		err = ctx.Err()
		if err == nil {
			err = context.Canceled
		}
	}
	m.mu.Lock()
	m.opening--
	if err == nil && m.closed {
		err = errs.New(errs.KindRequestUnavailable, "terminal service is shutting down")
	}
	if err != nil {
		replay.err = err
		close(replay.done)
		m.mu.Unlock()
		cancel()
		if process != nil {
			_ = process.Terminate()
		}
		return apiTypes.TerminalSession{}, "", err
	}
	target.Command = append([]string(nil), target.Command...)
	s := &session{
		manager:         m,
		owner:           owner,
		id:              id,
		pty:             process,
		target:          target,
		cancel:          cancel,
		terminationDone: make(chan struct{}),
	}
	s.expiresAt = time.Now().UTC().Add(m.inactivity)
	s.timer = time.AfterFunc(m.inactivity, s.expireInputInactivity)
	m.byID[id] = s
	replay.session = s
	close(replay.done)
	m.mu.Unlock()
	go s.readOutput()
	return s.projection(false), owner, nil
}

func (m *Manager) InputInactivitySeconds() int { return int(m.inactivity / time.Second) }

func (m *Manager) recordReplayLocked(key string, replay *terminalReplay) {
	if key == "" || len(m.replayOrder) >= maxReplayKeys {
		return
	}
	if _, exists := m.replays[key]; exists {
		return
	}
	m.replays[key] = replay
	m.replayOrder = append(m.replayOrder, key)
}

// ensureReplayCapacityLocked never evicts a key for a live shell: that key may
// be the only way to recover a lost Open response and its owner cookie.
func (m *Manager) ensureReplayCapacityLocked() bool {
	if len(m.replayOrder) < maxReplayKeys {
		return true
	}
	for index, key := range m.replayOrder {
		replay := m.replays[key]
		select {
		case <-replay.done:
		default:
			continue
		}
		if replay.session != nil && !replay.session.isClosed() {
			continue
		}
		delete(m.replays, key)
		copy(m.replayOrder[index:], m.replayOrder[index+1:])
		m.replayOrder = m.replayOrder[:len(m.replayOrder)-1]
		return true
	}
	return false
}

func (m *Manager) Attach(owner, id string, after uint64) (Attachment, error) {
	m.mu.Lock()
	s := m.findSessionLocked(id)
	m.mu.Unlock()
	if s == nil || s.owner != owner {
		return Attachment{}, errs.New(errs.KindRequestNotFound, "host terminal session was not found")
	}
	return s.attach(after)
}

func (m *Manager) findSessionLocked(id string) *session {
	if s := m.byID[id]; s != nil {
		return s
	}
	for _, replay := range m.replays {
		if replay.session != nil && replay.session.id == id {
			return replay.session
		}
	}
	return nil
}

func (m *Manager) Input(owner, id string, data []byte) (apiTypes.TerminalSession, error) {
	s, err := m.owned(owner, id)
	if err != nil {
		return apiTypes.TerminalSession{}, err
	}
	if len(data) == 0 || len(data) > terminalsession.MaximumFrameBytes {
		return apiTypes.TerminalSession{}, errs.New(errs.KindMalformedRequest, "terminal input size is invalid")
	}
	projection := s.markInput()
	if _, err := s.pty.Write(data); err != nil {
		s.terminate("shell_exited")
		return apiTypes.TerminalSession{}, errs.Wrap(errs.KindRequestFailed, err)
	}
	return projection, nil
}

func (m *Manager) Resize(owner, id string, columns, rows uint16) error {
	s, err := m.owned(owner, id)
	if err != nil {
		return err
	}
	return s.pty.Resize(terminalsession.Size{Columns: columns, Rows: rows})
}

func (m *Manager) Terminate(owner, id, reason string) error {
	s, err := m.owned(owner, id)
	if err != nil {
		return err
	}
	s.terminate(reason)
	return nil
}

func (m *Manager) owned(owner, id string) (*session, error) {
	m.mu.Lock()
	s := m.byID[id]
	m.mu.Unlock()
	if s == nil || owner == "" || s.owner != owner {
		return nil, errs.New(errs.KindRequestNotFound, "host terminal session was not found")
	}
	return s, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.cancel()
	sessions := make([]*session, 0, len(m.byID))
	for _, s := range m.byID {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.terminate("controller_shutdown")
	}
	m.cancel()
	return nil
}

func (m *Manager) release(s *session) {
	m.mu.Lock()
	if m.byID[s.id] == s {
		delete(m.byID, s.id)
	}
	m.mu.Unlock()
}

func (s *session) projection(resumed bool) apiTypes.TerminalSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.projectionLocked(resumed)
}

func (s *session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *session) projectionLocked(resumed bool) apiTypes.TerminalSession {
	state := "active"
	if s.closed {
		state = "terminated"
	}
	return apiTypes.TerminalSession{
		ID: s.id, Host: s.manager.hostname, User: s.target.User, State: state, Reason: s.reason, ExitCode: s.exitCode,
		Kind: s.target.Kind, ServiceID: s.target.ServiceID, ContainerID: s.target.ContainerID, ContainerName: s.target.ContainerName,
		InputInactivitySeconds: int(s.manager.inactivity / time.Second), ExpiresAt: s.expiresAt,
		StreamPath: "/api/v1/terminals/" + s.id + "/stream", Resumed: resumed,
	}
}

func (s *session) markInput() apiTypes.TerminalSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.projectionLocked(false)
	}
	now := time.Now().UTC()
	s.expiresAt = now.Add(s.manager.inactivity)
	s.timer.Reset(s.manager.inactivity)
	projection := s.projectionLocked(false)
	if now.Sub(s.lastActivityNotice) >= time.Second {
		s.lastActivityNotice = now
		s.sendLocked(Event{Kind: EventActivity, Session: projection})
	}
	return projection
}

func (s *session) expireInputInactivity() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	remaining := time.Until(s.expiresAt)
	if remaining > 0 {
		s.timer.Reset(remaining)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.terminate("input_inactivity")
}

func (s *session) attach(after uint64) (Attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attachment != nil {
		close(s.attachment)
	}
	events := make(chan Event, attachmentCapacity)
	s.attachment = events
	replay := make([]Event, 0, len(s.frames))
	truncated := len(s.frames) > 0 && after+1 < s.frames[0].Sequence
	for _, frame := range s.frames {
		if frame.Sequence > after {
			replay = append(
				replay,
				Event{Kind: EventOutput, Sequence: frame.Sequence, Data: append([]byte(nil), frame.Data...)},
			)
		}
	}
	if s.closed {
		replay = append(replay, Event{Kind: EventTerminated, Session: s.projectionLocked(true)})
		close(events)
		s.attachment = nil
		return Attachment{Replay: replay, Events: events, Truncated: truncated}, nil
	}
	return Attachment{
		Replay: replay, Events: events, Truncated: truncated,
		detach: func() { s.detach(events) },
	}, nil
}

func (s *session) detach(events chan Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attachment == events {
		close(events)
		s.attachment = nil
	}
}

func (s *session) readOutput() {
	buffer := make([]byte, outputReadBytes)
	for {
		count, err := s.pty.Read(buffer)
		if count > 0 {
			s.publish(buffer[:count])
		}
		if err != nil {
			_ = s.pty.Wait()
			s.terminate("shell_exited")
			return
		}
	}
}

func (s *session) publish(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.sequence++
	copyData := append([]byte(nil), data...)
	s.frames = append(s.frames, outputFrame{Sequence: s.sequence, Data: copyData})
	s.frameBytes += len(copyData)
	for s.frameBytes > maxReplayBytes && len(s.frames) > 0 {
		s.frameBytes -= len(s.frames[0].Data)
		s.frames[0].Data = nil
		s.frames = s.frames[1:]
	}
	s.sendLocked(Event{Kind: EventOutput, Sequence: s.sequence, Data: copyData})
}

func (s *session) sendLocked(event Event) {
	if s.attachment == nil {
		return
	}
	select {
	case s.attachment <- event:
	default:
		close(s.attachment)
		s.attachment = nil
	}
}

func (s *session) terminate(reason string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.terminationDone
		return
	}
	s.closed = true
	s.reason = reason
	s.timer.Stop()
	s.mu.Unlock()
	cleanupErr := s.pty.Terminate()
	result := s.pty.Result()
	s.cancel()
	s.mu.Lock()
	if cleanupErr != nil {
		s.reason = "cleanup_failed"
	} else if reason == "shell_exited" {
		s.reason = result.Reason
	}
	if result.Code >= 0 {
		code := result.Code
		s.exitCode = &code
	}
	projection := s.projectionLocked(false)
	if s.attachment != nil {
		for len(s.attachment) > 0 {
			<-s.attachment
		}
		s.attachment <- Event{Kind: EventTerminated, Session: projection}
		close(s.attachment)
		s.attachment = nil
	}
	s.mu.Unlock()
	s.manager.release(s)
	close(s.terminationDone)
}
