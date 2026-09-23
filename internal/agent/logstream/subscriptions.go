package logstream

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	maxLogSubscriptions = 8
	maxQueuedLogEvents  = 128
)

type Subscriptions struct {
	reader  agentprotocol.LogReader
	outputs chan *agentpb.AgentMessage

	mu     sync.Mutex
	active map[string]*activeLog
}

type activeLog struct {
	cancel  context.CancelFunc
	credits chan struct{}
}

func New(reader agentprotocol.LogReader) *Subscriptions {
	return &Subscriptions{
		reader:  reader,
		outputs: make(chan *agentpb.AgentMessage),
		active:  make(map[string]*activeLog),
	}
}

func (manager *Subscriptions) Outputs() <-chan *agentpb.AgentMessage {
	return manager.outputs
}

func (manager *Subscriptions) Subscribe(parent context.Context, request *agentpb.LogSubscribe) {
	if request == nil || request.GetRequestId() == "" || manager.reader == nil {
		return
	}
	if request.GetTail() > 1000 || len(request.GetTargets()) > 128 ||
		request.GetInitialCredit() == 0 || request.GetInitialCredit() > maxQueuedLogEvents {
		manager.send(
			parent,
			logEndMessage(request.GetRequestId(), agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED),
		)
		return
	}

	manager.mu.Lock()
	if _, exists := manager.active[request.GetRequestId()]; exists || len(manager.active) >= maxLogSubscriptions {
		manager.mu.Unlock()
		manager.send(parent, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_LogEnd{LogEnd: &agentpb.LogEnd{
			RequestId: request.GetRequestId(),
			Reason:    agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED,
		}}})
		return
	}
	subscriptionContext, cancel := context.WithCancel(parent)
	subscription := &activeLog{cancel: cancel, credits: make(chan struct{}, maxQueuedLogEvents)}
	for range request.GetInitialCredit() {
		subscription.credits <- struct{}{}
	}
	manager.active[request.GetRequestId()] = subscription
	manager.mu.Unlock()

	go manager.run(subscriptionContext, request, subscription)
}

func (manager *Subscriptions) Cancel(requestID string) {
	manager.mu.Lock()
	subscription := manager.active[requestID]
	manager.mu.Unlock()
	if subscription != nil {
		subscription.cancel()
	}
}

func (manager *Subscriptions) Grant(credit *agentpb.LogCredit) error {
	if credit == nil || credit.GetRequestId() == "" || credit.GetCount() == 0 ||
		credit.GetCount() > maxQueuedLogEvents {
		return errs.New(errs.KindValidationFailed, "log credit is invalid")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	subscription := manager.active[credit.GetRequestId()]
	if subscription == nil {
		return nil
	}
	if len(subscription.credits)+int(credit.GetCount()) > cap(subscription.credits) {
		return errs.New(errs.KindStateConflict, "log credit exceeds the subscription window")
	}
	for range credit.GetCount() {
		subscription.credits <- struct{}{}
	}
	return nil
}

func (manager *Subscriptions) run(ctx context.Context, request *agentpb.LogSubscribe, subscription *activeLog) {
	requestID := request.GetRequestId()
	released := false
	defer func() {
		if !released {
			manager.release(requestID)
		}
	}()

	sources, err := manager.reader.Open(ctx, request)
	if err != nil {
		reason := agentpb.LogEndReason_LOG_END_REASON_SOURCE_FAILED
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindStorageUnavailable {
			reason = agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED
		}
		manager.send(ctx, logEndMessage(requestID, reason))
		return
	}
	sourcesClosed := false
	defer func() {
		if !sourcesClosed {
			// Best-effort cleanup after pre-ready cancellation; no public stream exists.
			_ = sources.Close()
		}
	}()

	if !manager.send(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_LogReady{LogReady: &agentpb.LogReady{RequestId: requestID}},
		},
	) {
		return
	}

	runContext, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	incoming := make(chan *agentpb.LogEvent)
	events := make(chan *agentpb.LogEvent, maxQueuedLogEvents)
	sourceDone := make(chan error, 1)
	go func() {
		err := sources.Run(runContext, incoming)
		close(incoming)
		sourceDone <- err
	}()
	done := make(chan error, 1)
	go collectLogEvents(runContext, incoming, events, sourceDone, done)

loop:
	for event := range events {
		if event == nil {
			continue
		}
		select {
		case <-runContext.Done():
			break loop
		case <-subscription.credits:
		}
		if runContext.Err() != nil || !manager.send(runContext, &agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_LogEvent{LogEvent: event},
		}) {
			break
		}
	}
	runErr := <-done
	cancelRun()
	closeErr := sources.Close()
	sourcesClosed = true
	manager.release(requestID)
	released = true
	if ctx.Err() != nil {
		return
	}
	if runErr == nil {
		runErr = closeErr
	}
	reason := agentpb.LogEndReason_LOG_END_REASON_COMPLETE
	if runErr != nil {
		reason = agentpb.LogEndReason_LOG_END_REASON_SOURCE_FAILED
	}
	manager.send(ctx, logEndMessage(requestID, reason))
}

func collectLogEvents(
	ctx context.Context,
	incoming <-chan *agentpb.LogEvent,
	events chan<- *agentpb.LogEvent,
	sourceDone <-chan error,
	done chan<- error,
) {
	defer close(events)
	var result error
	cancelled := ctx.Done()
	for {
		select {
		case <-cancelled:
			result = ctx.Err()
			cancelled = nil
		case event, open := <-incoming:
			if !open {
				if sourceErr := <-sourceDone; result == nil {
					result = sourceErr
				}
				done <- result
				return
			}
			if result != nil {
				continue
			}
			select {
			case events <- event:
			case <-ctx.Done():
				result = ctx.Err()
				cancelled = nil
			}
		}
	}
}

func (manager *Subscriptions) release(requestID string) {
	manager.mu.Lock()
	delete(manager.active, requestID)
	manager.mu.Unlock()
}

func (manager *Subscriptions) send(ctx context.Context, message *agentpb.AgentMessage) bool {
	select {
	case manager.outputs <- message:
		return true
	case <-ctx.Done():
		return false
	}
}

func logEndMessage(requestID string, reason agentpb.LogEndReason) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_LogEnd{LogEnd: &agentpb.LogEnd{
		RequestId: requestID,
		Reason:    reason,
	}}}
}
