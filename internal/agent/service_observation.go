package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ServiceObserver reads the frozen serving targets without Task authority.
type ServiceObserver interface {
	Observe(context.Context, *agentpb.ObserveServices) (*agentpb.ServiceObservationResult, error)
}

func (c *Client) SetServiceObserver(observer ServiceObserver) error {
	if c == nil || observer == nil {
		return errs.New(errs.KindValidationFailed, "agent: service observer is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: service observer cannot change after start")
	}
	c.observer = observer
	return nil
}

// The stream loop alone owns admission and writes. Close and explicit cancel
// join the read before its slot can be reused; the worker never touches a Task.
type observationSession struct {
	observer  ServiceObserver
	outputs   chan *agentpb.ServiceObservationResult
	requestID string
	cancel    context.CancelFunc
	workers   sync.WaitGroup
}

func newObservationSession(observer ServiceObserver) *observationSession {
	return &observationSession{observer: observer, outputs: make(chan *agentpb.ServiceObservationResult, 1)}
}

func (session *observationSession) start(
	ctx context.Context, request *agentpb.ObserveServices,
) (*agentpb.ServiceObservationResult, error) {
	if err := serviceobservation.ValidateRequest(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if session.requestID != "" || session.observer == nil {
		return unavailableObservation(request), nil
	}
	owned := proto.CloneOf(request)
	ctx, cancel := context.WithTimeout(ctx, serviceobservation.Timeout)
	session.requestID, session.cancel = owned.RequestId, cancel
	session.workers.Add(1)
	go func() {
		defer session.workers.Done()
		defer cancel()
		result, err := session.observer.Observe(ctx, owned)
		if err != nil || ctx.Err() != nil || serviceobservation.ValidateResult(owned, result) != nil {
			result = unavailableObservation(owned)
		} else {
			// The stream takes ownership after the observer call returns.
			result = proto.CloneOf(result)
		}
		session.outputs <- result // One admitted worker, one buffered output.
	}()
	return nil, nil
}

func (session *observationSession) Close() error {
	if session.cancel != nil {
		session.cancel()
	}
	session.workers.Wait()
	session.requestID, session.cancel = "", nil
	select {
	case <-session.outputs:
	default:
	}
	return nil
}

func unavailableObservation(request *agentpb.ObserveServices) *agentpb.ServiceObservationResult {
	result := &agentpb.ServiceObservationResult{RequestId: request.RequestId}
	for _, target := range request.Targets {
		result.Observations = append(result.Observations, &agentpb.ServiceObservationRow{
			ServiceId: target.ServiceId, ReleaseId: target.ReleaseId,
			Outcome: &agentpb.ServiceObservationRow_Unavailable{Unavailable: true},
		})
	}
	return result
}

func (session *observationSession) handle(
	ctx context.Context, stream agentStream, message *agentpb.ControllerMessage,
) (bool, error) {
	request, cancel := message.GetObserveServices(), message.GetCancelServiceObservation()
	if request == nil && cancel == nil {
		return false, nil
	}
	if proto.Size(message) > serviceobservation.MaximumEnvelopeBytes || len(message.ProtoReflect().GetUnknown()) != 0 {
		return true, errs.New(errs.KindValidationFailed, "agent: invalid service observation envelope")
	}
	if cancel != nil {
		if err := serviceobservation.ValidateCancel(cancel); err != nil {
			return true, err
		}
		if session.requestID == cancel.RequestId {
			return true, session.Close()
		}
		return true, nil
	}
	result, err := session.start(ctx, request)
	if err != nil || result == nil {
		return true, err
	}
	return true, sendServiceObservation(ctx, stream, result)
}

func sendServiceObservation(ctx context.Context, stream agentStream, result *agentpb.ServiceObservationResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceObservationResult{
		ServiceObservationResult: result,
	}})
}
