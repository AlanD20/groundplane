package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type observationCommand struct {
	request *agentpb.ObserveServices
	ctx     context.Context
	result  chan observationReply
}

type observationReply struct {
	result *agentpb.ServiceObservationResult
	err    error
}

// ObserveServices captures one authenticated connection, never retries, and
// returns only machine evidence. Serving-source and freshness checks belong to
// the caller; this exchange neither creates Tasks nor claims public health.
func (r *Registry) ObserveServices(
	ctx context.Context, agentID string, targets []*agentpb.ServiceObservationTarget,
) (*agentpb.ServiceObservationResult, error) {
	if ctx == nil || r == nil {
		return nil, errs.New(errs.KindInternal, "service observation context and registry are required")
	}
	ctx, cancel := context.WithTimeout(ctx, serviceobservation.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := &agentpb.ObserveServices{RequestId: ids.NewULID(), Targets: targets}
	if err := serviceobservation.ValidateRequest(request); err != nil {
		return nil, err
	}
	command := &observationCommand{ctx: ctx, request: proto.CloneOf(request), result: make(chan observationReply, 1)}
	r.mu.Lock()
	state := r.agents[agentID]
	if state == nil || !state.online || state.revoked || closed(state.done) {
		r.mu.Unlock()
		return nil, observationUnavailable()
	}
	if state.activeObservation != nil {
		r.mu.Unlock()
		return nil, errs.New(errs.KindResourceInUse, "agent service observation is busy")
	}
	state.activeObservation = command
	r.mu.Unlock()
	defer func() {
		cancel()
		r.mu.Lock()
		if state.activeObservation == command {
			state.activeObservation = nil
		}
		r.mu.Unlock()
	}()
	select {
	case state.observationCommands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, observationUnavailable()
	}
	select {
	case reply := <-command.result:
		r.mu.Lock()
		current := r.agents[agentID] == state && state.online && !state.revoked && !closed(state.done)
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !current {
			return nil, observationUnavailable()
		}
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return nil, observationUnavailable()
	}
}

func observationUnavailable() error {
	return errs.New(errs.KindStorageUnavailable, "agent service observation is unavailable")
}

func (s *Session) observationCommandCurrent(ctx context.Context, command *observationCommand) bool {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	return ctx.Err() == nil && s.registry.agents[s.agentID] == s.state && s.state.online && !s.state.revoked &&
		!closed(s.state.done) && s.state.activeObservation == command && command.ctx.Err() == nil
}

func (s *Session) acceptObservationResult(ctx context.Context, message *agentpb.AgentMessage) {
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	active := s.state.activeObservation
	result := message.GetServiceObservationResult()
	if ctx.Err() != nil || s.registry.agents[s.agentID] != s.state || !s.state.online || s.state.revoked ||
		closed(s.state.done) || active == nil || active.ctx.Err() != nil || result == nil ||
		result.RequestId != active.request.RequestId {
		return
	}
	err := serviceobservation.ValidateResult(active.request, result)
	if proto.Size(message) > serviceobservation.MaximumEnvelopeBytes || len(message.ProtoReflect().GetUnknown()) != 0 {
		err = errs.New(errs.KindValidationFailed, "invalid service observation envelope")
	}
	reply := observationReply{err: err}
	if err == nil {
		reply.result = proto.CloneOf(result)
	}
	select {
	case active.result <- reply:
	default:
	}
}

// observationExchange belongs only to the stream loop. A retired read is
// cancelled on the same ordered stream before a replacement can be sent.
type observationExchange struct{ active *observationCommand }

func (exchange *observationExchange) done() <-chan struct{} {
	if exchange.active == nil {
		return nil
	}
	return exchange.active.ctx.Done()
}

func (exchange *observationExchange) cancel(ctx context.Context, stream agentpb.AgentChannel_ConnectServer) error {
	if exchange.active == nil {
		return nil
	}
	requestID := exchange.active.request.RequestId
	exchange.active = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CancelServiceObservation{
		CancelServiceObservation: &agentpb.CancelServiceObservation{RequestId: requestID},
	}})
}

func (exchange *observationExchange) send(
	ctx context.Context, stream agentpb.AgentChannel_ConnectServer, session *Session, command *observationCommand,
) error {
	if !session.observationCommandCurrent(ctx, command) {
		return nil
	}
	if err := exchange.cancel(ctx, stream); err != nil {
		return err
	}
	if !session.observationCommandCurrent(ctx, command) {
		return nil
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ObserveServices{
		ObserveServices: command.request,
	}}); err != nil {
		return err
	}
	exchange.active = command
	return nil
}
