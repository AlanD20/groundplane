// Package hostterminal owns transient, content-free metadata and the bounded
// byte transport for interactive host PTYs. Sessions are deliberately not
// Tasks and are never written to the durable store or application logs.
package hostterminal

import (
	"context"
	"errors"
	"io"
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
	Session  apiTypes.HostTerminalSession
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
	replays     map[string]terminalReplay
	replayOrder []string
}

type terminalReplay struct {
	owner   string
	columns uint16
	rows    uint16
	session *session
}

type session struct {
	manager *Manager
	owner   string
	id      string
	pty     *runner.HostPTY

	mu                 sync.Mutex
	expiresAt          time.Time
	timer              *time.Timer
	frames             []outputFrame
	frameBytes         int
	sequence           uint64
	closed             bool
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
		replays: make(map[string]terminalReplay),
	}, nil
}

func (m *Manager) Open(
	ctx context.Context,
	owner string,
	idempotencyKey string,
	columns uint16,
	rows uint16,
) (apiTypes.HostTerminalSession, string, error) {
	if err := ctx.Err(); err != nil {
		return apiTypes.HostTerminalSession{}, "", err
	}
	if idempotencyKey == "" {
		return apiTypes.HostTerminalSession{}, "", errs.New(
			errs.KindMalformedRequest,
			"terminal request identity is unavailable",
		)
	}
	m.mu.Lock()
	if replay, ok := m.replays[idempotencyKey]; ok {
		if replay.columns != columns || replay.rows != rows || owner != "" && replay.owner != owner {
			m.mu.Unlock()
			return apiTypes.HostTerminalSession{}, "", errs.New(
				errs.KindIdempotencyMismatch,
				"Idempotency-Key was already used for a different host terminal request",
			)
		}
		m.mu.Unlock()
		return replay.session.projection(true), replay.owner, nil
	}
	if m.closed {
		m.mu.Unlock()
		return apiTypes.HostTerminalSession{}, "", errs.New(
			errs.KindRequestUnavailable,
			"host terminal service is shutting down",
		)
	}
	if !m.ensureReplayCapacityLocked() {
		m.mu.Unlock()
		return apiTypes.HostTerminalSession{}, "", errs.New(
			errs.KindStateConflict,
			"host terminal request replay limit reached",
		)
	}
	if owner == "" {
		owner = ids.NewULID()
	}
	if len(m.byID) >= MaxSessions {
		m.mu.Unlock()
		return apiTypes.HostTerminalSession{}, "", errs.New(
			errs.KindStateConflict,
			"host terminal session limit reached",
		)
	}

	id := ids.New(ids.KindTerminal)
	ptyProcess, err := m.launcher.StartHostShell(m.ctx, id, runner.HostShellSize{Columns: columns, Rows: rows})
	if err != nil {
		m.mu.Unlock()
		return apiTypes.HostTerminalSession{}, "", err
	}
	s := &session{manager: m, owner: owner, id: id, pty: ptyProcess}
	s.expiresAt = time.Now().UTC().Add(m.inactivity)
	s.timer = time.AfterFunc(m.inactivity, s.expireInputInactivity)
	m.byID[s.id] = s
	m.recordReplayLocked(idempotencyKey, terminalReplay{
		owner: owner, columns: columns, rows: rows, session: s,
	})
	m.mu.Unlock()

	go s.readOutput()
	go func() {
		_ = ptyProcess.Wait()
		s.terminate("shell_exited")
	}()
	return s.projection(false), owner, nil
}

func (m *Manager) recordReplayLocked(key string, replay terminalReplay) {
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
		if !replay.session.isClosed() {
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
	s := m.byID[id]
	m.mu.Unlock()
	if s == nil || s.owner != owner {
		return Attachment{}, errs.New(errs.KindRequestNotFound, "host terminal session was not found")
	}
	return s.attach(after)
}

func (m *Manager) Input(owner, id string, data []byte) (apiTypes.HostTerminalSession, error) {
	s, err := m.owned(owner, id)
	if err != nil {
		return apiTypes.HostTerminalSession{}, err
	}
	if len(data) == 0 || len(data) > 32<<10 {
		return apiTypes.HostTerminalSession{}, errs.New(errs.KindMalformedRequest, "terminal input size is invalid")
	}
	projection := s.markInput()
	if _, err := s.pty.Write(data); err != nil {
		s.terminate("shell_exited")
		return apiTypes.HostTerminalSession{}, errs.Wrap(errs.KindRequestFailed, err)
	}
	return projection, nil
}

func (m *Manager) Resize(owner, id string, columns, rows uint16) error {
	s, err := m.owned(owner, id)
	if err != nil {
		return err
	}
	return s.pty.Resize(runner.HostShellSize{Columns: columns, Rows: rows})
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

func (s *session) projection(resumed bool) apiTypes.HostTerminalSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.projectionLocked(resumed)
}

func (s *session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *session) projectionLocked(resumed bool) apiTypes.HostTerminalSession {
	state := "active"
	if s.closed {
		state = "terminated"
	}
	return apiTypes.HostTerminalSession{
		ID: s.id, Host: s.manager.hostname, User: "root", State: state, Reason: s.reason,
		InputInactivitySeconds: int(s.manager.inactivity / time.Second), ExpiresAt: s.expiresAt,
		StreamPath: "/api/v1/host/terminal/" + s.id + "/stream", Resumed: resumed,
	}
}

func (s *session) markInput() apiTypes.HostTerminalSession {
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
	if s.closed {
		return Attachment{}, errs.New(errs.KindRequestNotFound, "host terminal session was not found")
	}
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
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) {
				s.terminate("shell_exited")
			}
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
		return
	}
	s.closed = true
	s.reason = reason
	s.timer.Stop()
	projection := s.projectionLocked(false)
	if s.attachment != nil {
		for len(s.attachment) > 0 {
			<-s.attachment
		}
		s.attachment <- Event{Kind: EventTerminated, Session: projection}
		close(s.attachment)
		s.attachment = nil
	}
	s.frames = nil
	s.frameBytes = 0
	s.mu.Unlock()
	_ = s.pty.Terminate()
	s.manager.release(s)
}
