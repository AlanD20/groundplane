package agentchannel

import (
	"context"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type edgeCheckCommand struct {
	request *agentpb.CheckEdgeConnections
	ctx     context.Context
	result  chan edgeCheckReply
}

type edgeCheckReply struct {
	result *agentpb.EdgeConnectionResult
	err    error
}

// CheckEdgeConnections binds one exchange to one authenticated connection.
// Its caller owns durable Task admission and possible helper effects; neither
// this exchange nor reconnect repeats the probe.
func (registry *Registry) CheckEdgeConnections(
	ctx context.Context,
	agentID string,
	input *agentpb.CheckEdgeConnections,
) (*agentpb.EdgeConnectionResult, error) {
	if ctx == nil || registry == nil || input == nil || ids.Validate(ids.KindAgent, agentID) != nil {
		return nil, errs.New(errs.KindInternal, "Tunnel check exchange context, Agent and input are required")
	}
	owned := proto.CloneOf(input)
	owned.RequestId = ids.NewULID()
	if err := contract.ValidateMachineRequest(owned); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, contract.ExchangeTimeout)
	defer cancel()
	command := &edgeCheckCommand{ctx: ctx, request: owned, result: make(chan edgeCheckReply, 1)}
	registry.mu.Lock()
	state := registry.agents[agentID]
	if state == nil || !state.online || state.revoked || closed(state.done) {
		registry.mu.Unlock()
		return nil, edgeCheckUnavailable()
	}
	if state.activeEdgeCheck != nil {
		registry.mu.Unlock()
		return nil, errs.New(errs.KindResourceInUse, "Agent Tunnel check is busy")
	}
	state.activeEdgeCheck = command
	registry.mu.Unlock()
	defer func() {
		cancel()
		registry.mu.Lock()
		if state.activeEdgeCheck == command {
			state.activeEdgeCheck = nil
		}
		registry.mu.Unlock()
	}()
	select {
	case state.edgeCheckCommands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, edgeCheckUnavailable()
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
			return nil, edgeCheckUnavailable()
		}
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, edgeCheckUnavailable()
	}
}

func edgeCheckUnavailable() error {
	return errs.New(errs.KindStorageUnavailable, "Agent Tunnel check exchange is unavailable")
}

func (session *Session) edgeCheckCommandCurrent(command *edgeCheckCommand) bool {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	return session.ctx.Err() == nil && command.ctx.Err() == nil &&
		session.registry.agents[session.agentID] == session.state && session.state.online && !session.state.revoked &&
		!closed(session.state.done) && session.state.activeEdgeCheck == command
}

func (session *Session) acceptEdgeCheckResult(message *agentpb.AgentMessage) {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	active, result := session.state.activeEdgeCheck, message.GetEdgeConnectionResult()
	if session.ctx.Err() != nil || session.registry.agents[session.agentID] != session.state ||
		!session.state.online || session.state.revoked || closed(session.state.done) || active == nil ||
		active.ctx.Err() != nil || result == nil || result.RequestId != active.request.RequestId {
		return
	}
	err := contract.ValidateMachineResult(active.request, result)
	if proto.Size(message) > contract.MaximumResultBytes+16 || len(message.ProtoReflect().GetUnknown()) != 0 {
		err = errs.New(errs.KindValidationFailed, "Tunnel check result envelope is invalid")
	}
	reply := edgeCheckReply{err: err}
	if err == nil {
		reply.result = proto.CloneOf(result)
	}
	select {
	case active.result <- reply:
	default:
	}
}

// Cancel precedes replacement on the stream. Sent protobufs remain immutable;
// a fresh request ID prevents a previous probe satisfying cleanup-only Retry.
type edgeCheckExchange struct{ active *edgeCheckCommand }

func (exchange *edgeCheckExchange) done() <-chan struct{} {
	if exchange.active == nil {
		return nil
	}
	return exchange.active.ctx.Done()
}

func (exchange *edgeCheckExchange) cancel(ctx context.Context, stream agentpb.AgentChannel_ConnectServer) error {
	if exchange.active == nil {
		return nil
	}
	requestID := exchange.active.request.RequestId
	exchange.active = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CancelEdgeConnections{
		CancelEdgeConnections: &agentpb.CancelEdgeConnections{RequestId: requestID},
	}})
}

func (exchange *edgeCheckExchange) send(
	ctx context.Context,
	stream agentpb.AgentChannel_ConnectServer,
	session *Session,
	command *edgeCheckCommand,
) error {
	if !session.edgeCheckCommandCurrent(command) {
		return nil
	}
	if err := exchange.cancel(ctx, stream); err != nil {
		return err
	}
	if !session.edgeCheckCommandCurrent(command) {
		return nil
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CheckEdgeConnections{
		CheckEdgeConnections: command.request,
	}}); err != nil {
		return err
	}
	exchange.active = command
	return nil
}
