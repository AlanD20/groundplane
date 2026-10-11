package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type dnsCheckCommand struct {
	ctx     context.Context
	request *agentpb.CheckDNSConnections
	result  chan dnsCheckReply
}

type dnsCheckReply struct {
	result *agentpb.DNSConnectionResult
	err    error
}

// CheckDNSConnections freezes one authenticated connection and performs one
// read-only exchange. Reconnection never resubmits a check automatically.
func (registry *Registry) CheckDNSConnections(ctx context.Context, agentID string,
	input *agentpb.CheckDNSConnections,
) (*agentpb.DNSConnectionResult, error) {
	if registry == nil || ctx == nil || input == nil || ids.Validate(ids.KindAgent, agentID) != nil {
		return nil, errs.New(errs.KindInternal, "DNS check context, Agent and target are required")
	}
	owned := proto.CloneOf(input)
	if err := dnsconnectioncheck.ValidateRequest(owned); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, dnsconnectioncheck.ExchangeTimeout)
	defer cancel()
	command := &dnsCheckCommand{ctx: ctx, request: owned, result: make(chan dnsCheckReply, 1)}
	registry.mu.Lock()
	state := registry.agents[agentID]
	if state == nil || !state.online || state.revoked || closed(state.done) {
		registry.mu.Unlock()
		return nil, dnsCheckUnavailable()
	}
	if state.activeDNSCheck != nil {
		registry.mu.Unlock()
		return nil, errs.New(errs.KindResourceInUse, "Agent DNS connection check is busy")
	}
	state.activeDNSCheck = command
	registry.mu.Unlock()
	defer func() {
		cancel()
		registry.mu.Lock()
		if state.activeDNSCheck == command {
			state.activeDNSCheck = nil
		}
		registry.mu.Unlock()
	}()
	select {
	case state.dnsCheckCommands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, dnsCheckUnavailable()
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
			return nil, dnsCheckUnavailable()
		}
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, dnsCheckUnavailable()
	}
}

func dnsCheckUnavailable() error {
	return errs.New(errs.KindStorageUnavailable, "Agent DNS connection check is unavailable")
}

func (session *Session) dnsCheckCommandCurrent(command *dnsCheckCommand) bool {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	return session.ctx.Err() == nil && command.ctx.Err() == nil &&
		session.registry.agents[session.agentID] == session.state &&
		session.state.online &&
		!session.state.revoked &&
		!closed(session.state.done) &&
		session.state.activeDNSCheck == command
}

func (session *Session) acceptDNSCheckResult(message *agentpb.AgentMessage) {
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	active, result := session.state.activeDNSCheck, message.GetDnsConnectionResult()
	if session.ctx.Err() != nil || session.registry.agents[session.agentID] != session.state ||
		!session.state.online || session.state.revoked || closed(session.state.done) || active == nil ||
		active.ctx.Err() != nil || result == nil || result.RequestId != active.request.RequestId {
		return
	}
	err := dnsconnectioncheck.ValidateResult(active.request, result)
	if proto.Size(message) > dnsconnectioncheck.MaximumResultBytes+16 || executionplan.RejectUnknown(message) != nil {
		err = errs.New(errs.KindValidationFailed, "DNS check result envelope is invalid")
	}
	reply := dnsCheckReply{err: err}
	if err == nil {
		reply.result = proto.CloneOf(result)
	}
	select {
	case active.result <- reply:
	default:
	}
}

type dnsCheckExchange struct{ active *dnsCheckCommand }

func (exchange *dnsCheckExchange) done() <-chan struct{} {
	if exchange.active == nil {
		return nil
	}
	return exchange.active.ctx.Done()
}

func (exchange *dnsCheckExchange) cancel(ctx context.Context, stream agentpb.AgentChannel_ConnectServer) error {
	if exchange.active == nil {
		return nil
	}
	requestID := exchange.active.request.RequestId
	exchange.active = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CancelDnsConnections{
		CancelDnsConnections: &agentpb.CancelDNSConnections{RequestId: requestID},
	}})
}

func (exchange *dnsCheckExchange) send(ctx context.Context, stream agentpb.AgentChannel_ConnectServer,
	session *Session, command *dnsCheckCommand,
) error {
	if !session.dnsCheckCommandCurrent(command) {
		return nil
	}
	if err := exchange.cancel(ctx, stream); err != nil {
		return err
	}
	if !session.dnsCheckCommandCurrent(command) {
		return nil
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CheckDnsConnections{
		CheckDnsConnections: command.request,
	}}); err != nil {
		return err
	}
	exchange.active = command
	return nil
}
