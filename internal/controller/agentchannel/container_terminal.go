package agentchannel

import (
	"context"
	"io"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type terminalCommand struct {
	message *agentpb.ControllerMessage
	result  chan error
}

type containerProcess struct {
	registry   *Registry
	state      *sessionState
	id         string
	ready      chan error
	output     chan []byte
	done       chan struct{}
	finishOnce sync.Once
	stopOnce   sync.Once
	readMu     sync.Mutex
	pending    []byte
	readySeen  bool  // guarded by registry.mu
	err        error // published by closing done
	result     terminalsession.Exit
}

// OpenContainerShell captures the current sole Agent connection. It never
// redirects an established session after reconnection or a Service deployment.
func (r *Registry) OpenContainerShell(
	ctx context.Context,
	request *agentpb.ContainerTerminalOpen,
) (terminalsession.Process, error) {
	if _, err := agentprotocol.ContainerOptionsFromWire(request); err != nil {
		return nil, err
	}
	r.mu.Lock()
	var state *sessionState
	for _, candidate := range r.agents {
		if candidate.online && candidate.readyReported && !candidate.revoked {
			if state != nil {
				r.mu.Unlock()
				return nil, errs.New(errs.KindStateConflict, "container shell requires an unambiguous Agent host")
			}
			state = candidate
		}
	}
	if state == nil {
		r.mu.Unlock()
		return nil, errs.New(errs.KindRequestUnavailable, "Agent is unavailable for container shell")
	}
	if _, exists := r.terminals[request.SessionId]; exists || len(r.terminals) >= terminalsession.MaximumSessions {
		r.mu.Unlock()
		return nil, errs.New(errs.KindStateConflict, "container terminal session limit or duplicate identifier")
	}
	p := &containerProcess{
		registry: r,
		state:    state,
		id:       request.SessionId,
		ready:    make(chan error, 1),
		output:   make(chan []byte, 64),
		done:     make(chan struct{}),
	}
	r.terminals[p.id] = p
	r.mu.Unlock()
	go func() {
		select {
		case <-state.done:
			p.finish(errs.New(errs.KindRequestUnavailable, "Agent connection ended; container terminal closed"))
		case <-p.done:
		}
	}()
	startup, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	err := p.send(
		startup,
		&agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ContainerTerminalOpen{ContainerTerminalOpen: proto.CloneOf(request)},
		},
	)
	if err == nil {
		select {
		case err = <-p.ready:
		case <-startup.Done():
			err = startup.Err()
		}
	}
	if err != nil {
		_ = p.Terminate()
		return nil, err
	}
	return p, nil
}

func (p *containerProcess) send(ctx context.Context, message *agentpb.ControllerMessage) error {
	command := terminalCommand{message: message, result: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.state.done:
		return errs.New(errs.KindRequestUnavailable, "Agent terminal connection ended")
	case p.state.terminalCommands <- command:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.state.done:
		return errs.New(errs.KindRequestUnavailable, "Agent terminal connection ended")
	case err := <-command.result:
		return err
	}
}

func (p *containerProcess) control(frame *agentpb.ContainerTerminalControl) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	frame.SessionId = p.id
	return p.send(
		ctx,
		&agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ContainerTerminalControl{ContainerTerminalControl: frame},
		},
	)
}

func (p *containerProcess) finish(err error) {
	p.finishResult(err, terminalsession.Exit{Reason: "connection_closed", Code: -1})
}

func (p *containerProcess) finishResult(err error, result terminalsession.Exit) {
	p.finishOnce.Do(func() {
		p.err = err
		p.result = result
		select {
		case p.ready <- err:
		default:
		}
		close(p.done)
		p.registry.mu.Lock()
		delete(p.registry.terminals, p.id)
		p.registry.mu.Unlock()
	})
}

func (s *Session) RecordContainerTerminal(event *agentpb.ContainerTerminalEvent) error {
	r := s.registry
	r.mu.Lock()
	p := r.terminals[event.SessionId]
	if p == nil || p.state != s.state {
		r.mu.Unlock()
		return nil
	}
	var failure error
	closed := false
	result := terminalsession.Exit{Reason: "transport_failed", Code: -1}
	switch frame := event.Event.(type) {
	case *agentpb.ContainerTerminalEvent_Ready:
		if p.readySeen || !frame.Ready {
			failure = errs.New(errs.KindValidationFailed, "duplicate or invalid terminal readiness")
		} else {
			p.readySeen = true
			p.ready <- nil
		}
	case *agentpb.ContainerTerminalEvent_Output:
		if !p.readySeen || len(frame.Output) == 0 || len(frame.Output) > terminalsession.MaximumFrameBytes {
			failure = errs.New(errs.KindValidationFailed, "invalid terminal output")
		} else {
			select {
			case p.output <- append([]byte(nil), frame.Output...):
			default:
				failure = errs.New(errs.KindRequestUnavailable, "terminal output exceeded its bounded window")
			}
		}
	case *agentpb.ContainerTerminalEvent_Closed:
		closed = true
		if frame.Closed != nil && (len(frame.Closed.FailureDetail) > 512 || !utf8.ValidString(frame.Closed.FailureDetail) || frame.Closed.ExitCode < -1 || frame.Closed.ExitCode > 255) {
			r.mu.Unlock()
			return errs.New(errs.KindValidationFailed, "invalid terminal completion")
		}
		if frame.Closed != nil {
			result = terminalsession.Exit{Reason: frame.Closed.Reason, Code: int(frame.Closed.ExitCode)}
		}
		if frame.Closed == nil || !p.readySeen || (frame.Closed.Reason != "command_exited" && frame.Closed.Reason != "terminated" && frame.Closed.Reason != "input_inactivity") {
			failure = errs.New(errs.KindRequestUnavailable, "Container shell ended; check executable, user and working directory if it did not start")
			if frame.Closed != nil && frame.Closed.Reason == "startup_failed" && frame.Closed.FailureDetail != "" {
				failure = errs.New(errs.KindRequestUnavailable, frame.Closed.FailureDetail)
			}
		}
	default:
		failure = errs.New(errs.KindValidationFailed, "unknown terminal event")
	}
	r.mu.Unlock()
	if closed || failure != nil {
		p.finishResult(failure, result)
	}
	return failureIfProtocol(failure)
}

func failureIfProtocol(err error) error {
	if kind, ok := errs.KindOf(err); ok && kind == errs.KindValidationFailed {
		return err
	}
	return nil
}

func (p *containerProcess) Read(data []byte) (int, error) {
	p.readMu.Lock()
	defer p.readMu.Unlock()
	if len(data) == 0 {
		return 0, nil
	}
	if len(p.pending) == 0 {
		select {
		case p.pending = <-p.output:
		default:
		}
		if len(p.pending) == 0 {
			select {
			case p.pending = <-p.output:
			case <-p.done:
				select {
				case p.pending = <-p.output:
				default:
					if p.err != nil {
						return 0, p.err
					}
					return 0, io.EOF
				}
			}
		}
	}
	n := copy(data, p.pending)
	p.pending = p.pending[n:]
	if len(p.pending) == 0 {
		select {
		case <-p.done:
		default:
			if err := p.control(&agentpb.ContainerTerminalControl{Control: &agentpb.ContainerTerminalControl_Credit{Credit: 1}}); err != nil {
				p.finish(err)
			}
		}
	}
	return n, nil
}

func (p *containerProcess) Write(data []byte) (int, error) {
	if len(data) == 0 || len(data) > terminalsession.MaximumFrameBytes {
		return 0, errs.New(errs.KindValidationFailed, "container terminal input is invalid")
	}
	select {
	case <-p.done:
		return 0, io.ErrClosedPipe
	default:
	}
	if err := p.control(&agentpb.ContainerTerminalControl{Control: &agentpb.ContainerTerminalControl_Input{Input: append([]byte(nil), data...)}}); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (p *containerProcess) Resize(size terminalsession.Size) error {
	if err := size.Validate(); err != nil {
		return err
	}
	return p.control(
		&agentpb.ContainerTerminalControl{
			Control: &agentpb.ContainerTerminalControl_Size{
				Size: &agentpb.ContainerTerminalSize{Columns: uint32(size.Columns), Rows: uint32(size.Rows)},
			},
		},
	)
}

func (p *containerProcess) Wait() error { <-p.done; return p.err }

func (p *containerProcess) Result() terminalsession.Exit { <-p.done; return p.result }

func (p *containerProcess) Terminate() error {
	p.stopOnce.Do(func() {
		select {
		case <-p.done:
			return
		default:
		}
		err := p.control(
			&agentpb.ContainerTerminalControl{Control: &agentpb.ContainerTerminalControl_Terminate{Terminate: true}},
		)
		if err != nil {
			p.finish(err)
			return
		}
		select {
		case <-p.done:
		case <-time.After(6 * time.Second):
			p.finish(errs.New(errs.KindRequestUnavailable, "container shell termination was not acknowledged"))
		}
	})
	return p.Wait()
}
