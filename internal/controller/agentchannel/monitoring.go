package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/monitoringwire"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type monitoringCommand struct {
	request *agentpb.CollectMonitoringSamples
	ctx     context.Context
	result  chan monitoringReply
}

type monitoringReply struct {
	result *agentpb.MonitoringSamplesResult
	err    error
}

// CollectMonitoringSamples captures one authenticated connection, never retries, and
// returns only machine evidence. Serving-source and freshness checks belong to
// the caller; this exchange neither creates Tasks nor claims public health.
func (r *Registry) CollectMonitoring(

	ctx context.Context, agentID string, targets []*agentpb.ServiceObservationTarget,
	volumes []*agentpb.MonitoringVolumeTarget, includeStorage bool,
) (*agentpb.MonitoringSamplesResult, error) {
	if ctx == nil || r == nil {
		return nil, errs.New(errs.KindInternal, "monitoring sampling context and registry are required")
	}
	ctx, cancel := context.WithTimeout(ctx, monitoringwire.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := &agentpb.CollectMonitoringSamples{
		RequestId:      ids.NewULID(),
		Targets:        targets,
		Volumes:        volumes,
		IncludeStorage: includeStorage,
	}
	if err := monitoringwire.ValidateRequest(request); err != nil {
		return nil, err
	}
	command := &monitoringCommand{ctx: ctx, request: proto.CloneOf(request), result: make(chan monitoringReply, 1)}
	r.mu.Lock()
	state := r.agents[agentID]
	if state == nil || !state.online || state.revoked || closed(state.done) || !r.monitoringCompatibleLocked(state) {
		r.mu.Unlock()
		return nil, monitoringUnavailable()
	}
	if !r.auxiliaryAllowedLocked(agentID, state) {
		r.mu.Unlock()
		return nil, auxiliaryPaused()
	}
	if state.activeMonitoring != nil {
		r.mu.Unlock()
		return nil, errs.New(errs.KindResourceInUse, "agent monitoring sampling is busy")
	}
	state.activeMonitoring = command
	r.mu.Unlock()
	defer func() {
		cancel()
		r.mu.Lock()
		if state.activeMonitoring == command {
			state.activeMonitoring = nil
		}
		r.mu.Unlock()
	}()
	select {
	case state.monitoringCommands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, monitoringUnavailable()
	}
	select {
	case reply := <-command.result:
		r.mu.Lock()
		current := r.agents[agentID] == state && state.online && !state.revoked && !closed(state.done) &&
			r.monitoringCompatibleLocked(state)
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !current {
			return nil, monitoringUnavailable()
		}
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, monitoringUnavailable()
	}
}

func monitoringUnavailable() error {
	return errs.New(errs.KindStorageUnavailable, "agent monitoring sampling is unavailable")
}

func (s *Session) monitoringCommandCurrent(ctx context.Context, command *monitoringCommand) bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	return s.registry.monitoringCompatibleLocked(s.state) && ctx.Err() == nil &&
		s.registry.agents[s.agentID] == s.state &&
		s.state.online &&
		!s.state.revoked &&
		!closed(s.state.done) &&
		s.state.activeMonitoring == command &&
		command.ctx.Err() == nil
}

func (s *Session) acceptMonitoringResult(ctx context.Context, message *agentpb.AgentMessage) {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	active := s.state.activeMonitoring
	result := message.GetMonitoringSamplesResult()
	if ctx.Err() != nil || s.registry.agents[s.agentID] != s.state || !s.state.online || s.state.revoked ||
		closed(
			s.state.done,
		) || !s.registry.monitoringCompatibleLocked(s.state) || active == nil || active.ctx.Err() != nil || result == nil ||
		result.RequestId != active.request.RequestId {
		return
	}
	err := monitoringwire.ValidateResult(active.request, result)
	if proto.Size(message) > monitoringwire.MaximumResultBytes || len(message.ProtoReflect().GetUnknown()) != 0 {
		err = errs.New(errs.KindValidationFailed, "invalid monitoring sampling envelope")
	}
	reply := monitoringReply{err: err}
	if err == nil {
		reply.result = proto.CloneOf(result)
	}
	select {
	case active.result <- reply:
	default:
	}
}

// monitoringExchange belongs only to the stream loop. A retired read is
// cancelled on the same ordered stream before a replacement can be sent.
type monitoringExchange struct{ active *monitoringCommand }

func (exchange *monitoringExchange) done() <-chan struct{} {
	if exchange.active == nil {
		return nil
	}
	return exchange.active.ctx.Done()
}

func (exchange *monitoringExchange) cancel(ctx context.Context, stream agentpb.AgentChannel_ConnectServer) error {
	if exchange.active == nil {
		return nil
	}
	requestID := exchange.active.request.RequestId
	exchange.active = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CancelMonitoringSamples{
		CancelMonitoringSamples: &agentpb.CancelMonitoringSamples{RequestId: requestID},
	}})
}

func (exchange *monitoringExchange) send(
	ctx context.Context, stream agentpb.AgentChannel_ConnectServer, session *Session, command *monitoringCommand,
) error {
	if !session.monitoringCommandCurrent(ctx, command) {
		return nil
	}
	if err := exchange.cancel(ctx, stream); err != nil {
		return err
	}
	if !session.monitoringCommandCurrent(ctx, command) {
		return nil
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CollectMonitoringSamples{
		CollectMonitoringSamples: command.request,
	}}); err != nil {
		return err
	}
	exchange.active = command
	return nil
}

func (r *Registry) monitoringCompatibleLocked(state *sessionState) bool {
	return state != nil && state.readyReported && r.source.Known() && r.source.Validate() == nil &&
		state.source == r.source
}
