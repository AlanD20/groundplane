// Package containerterminal owns transient, bounded container interactions on
// one authenticated Agent connection. Disconnect never recreates a process.
package containerterminal

import (
	"context"
	"errors"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Opener interface {
	Open(context.Context, *agentpb.ContainerTerminalOpen) (terminalsession.Process, error)
}

type Session struct {
	ctx     context.Context
	cancel  context.CancelFunc
	opener  Opener
	outputs chan *agentpb.AgentMessage
	mu      sync.Mutex
	active  map[string]*interaction
	workers sync.WaitGroup
}

type interaction struct {
	cancel   context.CancelFunc
	controls chan *agentpb.ContainerTerminalControl
	credits  chan struct{}
}

func New(ctx context.Context, opener Opener) *Session {
	owned, cancel := context.WithCancel(ctx)
	return &Session{
		ctx:     owned,
		cancel:  cancel,
		opener:  opener,
		outputs: make(chan *agentpb.AgentMessage),
		active:  make(map[string]*interaction),
	}
}

func (s *Session) Outputs() <-chan *agentpb.AgentMessage { return s.outputs }

func (s *Session) Open(request *agentpb.ContainerTerminalOpen) error {
	if _, err := agentprotocol.ContainerOptionsFromWire(request); err != nil {
		return err
	}
	if s.opener == nil {
		return errs.New(errs.KindRequestUnavailable, "container terminal is unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.active[request.SessionId]; exists || len(s.active) >= terminalsession.MaximumSessions {
		return errs.New(errs.KindStateConflict, "container terminal session limit or duplicate identifier")
	}
	ctx, cancel := context.WithCancel(s.ctx)
	i := &interaction{
		cancel:   cancel,
		controls: make(chan *agentpb.ContainerTerminalControl, 16),
		credits:  make(chan struct{}, 64),
	}
	for range request.InitialCredit {
		i.credits <- struct{}{}
	}
	s.active[request.SessionId] = i
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		defer cancel()
		defer func() { s.mu.Lock(); delete(s.active, request.SessionId); s.mu.Unlock() }()
		s.run(ctx, request, i)
	}()
	return nil
}

func (s *Session) Control(control *agentpb.ContainerTerminalControl) error {
	if control == nil || control.SessionId == "" {
		return errs.New(errs.KindValidationFailed, "container terminal control is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.active[control.SessionId]
	if i == nil {
		return nil
	}
	switch frame := control.Control.(type) {
	case *agentpb.ContainerTerminalControl_Credit:
		if frame.Credit == 0 || frame.Credit > 64 || len(i.credits)+int(frame.Credit) > cap(i.credits) {
			return errs.New(errs.KindValidationFailed, "container terminal credit exceeds its window")
		}
		for range frame.Credit {
			i.credits <- struct{}{}
		}
		return nil
	case *agentpb.ContainerTerminalControl_Terminate:
		if !frame.Terminate {
			return errs.New(errs.KindValidationFailed, "container terminal termination is invalid")
		}
		i.cancel()
		return nil
	case *agentpb.ContainerTerminalControl_Input:
		if len(frame.Input) == 0 || len(frame.Input) > terminalsession.MaximumFrameBytes {
			return errs.New(errs.KindValidationFailed, "container terminal input is invalid")
		}
	case *agentpb.ContainerTerminalControl_Size:
		if frame.Size == nil || frame.Size.Columns > 500 || frame.Size.Rows > 300 ||
			(terminalsession.Size{Columns: uint16(frame.Size.Columns), Rows: uint16(frame.Size.Rows)}).Validate() != nil {
			return errs.New(errs.KindValidationFailed, "container terminal size is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "container terminal control is unknown")
	}
	select {
	case i.controls <- control:
		return nil
	default:
		i.cancel()
		return errs.New(errs.KindRequestUnavailable, "container terminal input queue is full")
	}
}

func (s *Session) Close() {
	s.cancel()
	s.mu.Lock()
	for _, i := range s.active {
		i.cancel()
	}
	s.mu.Unlock()
	s.workers.Wait()
}

func (s *Session) send(ctx context.Context, event *agentpb.ContainerTerminalEvent) bool {
	select {
	case s.outputs <- &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ContainerTerminalEvent{ContainerTerminalEvent: event}}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Session) run(ctx context.Context, request *agentpb.ContainerTerminalOpen, i *interaction) {
	process, err := s.opener.Open(ctx, request)
	if err != nil {
		detail := "Container shell could not start"
		var public *errs.Error
		if errors.As(err, &public) {
			detail = public.ToProblem().Detail
		}
		s.send(
			ctx,
			&agentpb.ContainerTerminalEvent{
				SessionId: request.SessionId,
				Event: &agentpb.ContainerTerminalEvent_Closed{
					Closed: &agentpb.ContainerTerminalClosed{
						Reason:        "startup_failed",
						FailureDetail: detail,
						ExitCode:      -1,
					},
				},
			},
		)
		return
	}
	defer func() {
		_ = process.Terminate()
		result := process.Result()
		s.send(
			s.ctx,
			&agentpb.ContainerTerminalEvent{
				SessionId: request.SessionId,
				Event: &agentpb.ContainerTerminalEvent_Closed{
					Closed: &agentpb.ContainerTerminalClosed{Reason: result.Reason, ExitCode: int32(result.Code)},
				},
			},
		)
	}()
	if !s.send(
		ctx,
		&agentpb.ContainerTerminalEvent{
			SessionId: request.SessionId,
			Event:     &agentpb.ContainerTerminalEvent_Ready{Ready: true},
		},
	) {
		return
	}
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		for {
			select {
			case <-ctx.Done():
				_ = process.Terminate()
				return
			case control := <-i.controls:
				var err error
				if frame, ok := control.Control.(*agentpb.ContainerTerminalControl_Input); ok {
					_, err = process.Write(frame.Input)
				} else {
					size := control.GetSize()
					err = process.Resize(terminalsession.Size{Columns: uint16(size.Columns), Rows: uint16(size.Rows)})
				}
				if err != nil {
					i.cancel()
					_ = process.Terminate()
					return
				}
			}
		}
	}()
	defer func() { i.cancel(); <-controlDone }()
	buffer := make([]byte, terminalsession.MaximumFrameBytes)
	for {
		n, readErr := process.Read(buffer)
		if n > 0 {
			select {
			case <-i.credits:
			case <-ctx.Done():
				return
			}
			if !s.send(
				ctx,
				&agentpb.ContainerTerminalEvent{
					SessionId: request.SessionId,
					Event:     &agentpb.ContainerTerminalEvent_Output{Output: append([]byte(nil), buffer[:n]...)},
				},
			) {
				return
			}
		}
		if readErr != nil {
			_ = process.Wait()
			return
		}
	}
}
