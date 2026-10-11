package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type backingAccessCommand struct {
	request *agentpb.CheckBackingAccess
	ctx     context.Context
	result  chan backingAccessReply
}

type backingAccessReply struct {
	result *agentpb.BackingAccessResult
	err    error
}

// CheckBackingAccess captures one authenticated Agent connection and never
// retries the probe. The owning Controller Task persists its captured input and
// possible helper ownership before calling this bounded exchange.
func (registry *Registry) CheckBackingAccess(ctx context.Context, agentID string,
	input *agentpb.CheckBackingAccess,
) (*agentpb.BackingAccessResult, error) {
	if ctx == nil || registry == nil || input == nil || ids.Validate(ids.KindAgent, agentID) != nil {
		return nil, errs.New(errs.KindInternal, "Backing access exchange context, Agent and input are required")
	}
	owned := proto.CloneOf(input)
	owned.RequestId = ids.NewULID()
	handedOff := false
	defer func() {
		if !handedOff {
			backingaccess.ClearMachineRequest(owned)
		}
	}()
	if err := backingaccess.ValidateMachineRequest(owned); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, backingaccess.ExchangeTimeout)
	defer cancel()
	command := &backingAccessCommand{ctx: ctx, request: owned, result: make(chan backingAccessReply, 1)}
	registry.mu.Lock()
	state := registry.agents[agentID]
	if state == nil || !state.online || state.revoked || closed(state.done) {
		registry.mu.Unlock()
		return nil, backingAccessUnavailable()
	}
	if state.activeBackingAccess != nil {
		registry.mu.Unlock()
		return nil, errs.New(errs.KindResourceInUse, "Agent Backing access check is busy")
	}
	state.activeBackingAccess = command
	registry.mu.Unlock()
	defer func() {
		cancel()
		registry.mu.Lock()
		if state.activeBackingAccess == command {
			state.activeBackingAccess = nil
		}
		registry.mu.Unlock()
	}()
	select {
	case state.backingAccessCommands <- command:
		handedOff = true
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, backingAccessUnavailable()
	}
	select {
	case reply := <-command.result:
		registry.mu.Lock()
		current := registry.agents[agentID] == state && state.online && !state.revoked && !closed(state.done)
		registry.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !current {
			return nil, backingAccessUnavailable()
		}
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, backingAccessUnavailable()
	}
}

func backingAccessUnavailable() error {
	return errs.New(errs.KindStorageUnavailable, "Agent Backing access exchange is unavailable")
}

func (session *Session) backingAccessCommandCurrent(command *backingAccessCommand) bool {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	return session.ctx.Err() == nil && command.ctx.Err() == nil &&
		session.registry.agents[session.agentID] == session.state && session.state.online && !session.state.revoked &&
		!closed(session.state.done) && session.state.activeBackingAccess == command
}

func (session *Session) acceptBackingAccessResult(message *agentpb.AgentMessage) {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	active := session.state.activeBackingAccess
	result := message.GetBackingAccessResult()
	if session.ctx.Err() != nil || session.registry.agents[session.agentID] != session.state ||
		!session.state.online || session.state.revoked || closed(session.state.done) || active == nil ||
		active.ctx.Err() != nil || result == nil || result.RequestId != active.request.RequestId {
		return
	}
	err := backingaccess.ValidateMachineResult(active.request, result)
	if proto.Size(message) > backingaccess.MaximumResponseBytes+16 || len(message.ProtoReflect().GetUnknown()) != 0 {
		err = errs.New(errs.KindValidationFailed, "Backing access result envelope is invalid")
	}
	reply := backingAccessReply{err: err}
	if err == nil {
		reply.result = proto.CloneOf(result)
	}
	select {
	case active.result <- reply:
	default:
	}
}

// The stream writer alone owns the request after handoff. Unsent private input
// is cleared; a protobuf handed to gRPC stays immutable and becomes unreachable
// when this bounded exchange retires. Cancel precedes replacement.
type backingAccessExchange struct{ active *backingAccessCommand }

func (exchange *backingAccessExchange) done() <-chan struct{} {
	if exchange.active == nil {
		return nil
	}
	return exchange.active.ctx.Done()
}

func (exchange *backingAccessExchange) cancel(ctx context.Context, stream agentpb.AgentChannel_ConnectServer) error {
	if exchange.active == nil {
		return nil
	}
	requestID := exchange.active.request.RequestId
	exchange.active = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CancelBackingAccess{
		CancelBackingAccess: &agentpb.CancelBackingAccess{RequestId: requestID},
	}})
}

func (exchange *backingAccessExchange) send(ctx context.Context, stream agentpb.AgentChannel_ConnectServer,
	session *Session, command *backingAccessCommand,
) error {
	sent := false
	defer func() {
		if !sent {
			backingaccess.ClearMachineRequest(command.request)
		}
	}()
	if !session.backingAccessCommandCurrent(command) {
		return nil
	}
	if err := exchange.cancel(ctx, stream); err != nil {
		return err
	}
	if !session.backingAccessCommandCurrent(command) {
		return nil
	}
	// gRPC may retain a sent message for lazy stats/tracing. Never mutate it,
	// even when Send returns an error. Public logs never receive this message.
	sent = true
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CheckBackingAccess{
		CheckBackingAccess: command.request,
	}}); err != nil {
		return err
	}
	exchange.active = command
	return nil
}
