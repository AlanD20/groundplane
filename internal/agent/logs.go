package agent

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

type logManager struct {
	reader  agentprotocol.LogReader
	outputs chan *agentpb.AgentMessage

	mu     sync.Mutex
	active map[string]context.CancelFunc
}

func newLogManager(reader agentprotocol.LogReader) *logManager {
	return &logManager{
		reader:  reader,
		outputs: make(chan *agentpb.AgentMessage),
		active:  make(map[string]context.CancelFunc),
	}
}

func (manager *logManager) Outputs() <-chan *agentpb.AgentMessage {
	return manager.outputs
}

func (manager *logManager) Subscribe(parent context.Context, request *agentpb.LogSubscribe) {
	if request == nil || request.GetRequestId() == "" || manager.reader == nil {
		return
	}
	if request.GetTail() > 1000 || len(request.GetTargets()) > 128 {
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
	manager.active[request.GetRequestId()] = cancel
	manager.mu.Unlock()

	go manager.run(subscriptionContext, request)
}

func (manager *logManager) Cancel(requestID string) {
	manager.mu.Lock()
	cancel := manager.active[requestID]
	manager.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (manager *logManager) run(ctx context.Context, request *agentpb.LogSubscribe) {
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
	slots := make(chan struct{}, maxQueuedLogEvents)
	sourceDone := make(chan error, 1)
	go func() {
		err := sources.Run(runContext, incoming)
		close(incoming)
		sourceDone <- err
	}()
	done := make(chan error, 1)
	go collectLogEvents(cancelRun, incoming, events, slots, sourceDone, done)

	for event := range events {
		if event != nil && !manager.send(runContext, &agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_LogEvent{LogEvent: event},
		}) {
			<-slots
			break
		}
		<-slots
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
	cancel context.CancelFunc,
	incoming <-chan *agentpb.LogEvent,
	events chan<- *agentpb.LogEvent,
	slots chan<- struct{},
	sourceDone <-chan error,
	done chan<- error,
) {
	var result error
	for event := range incoming {
		if result != nil {
			continue
		}
		select {
		case slots <- struct{}{}:
			events <- event
		default:
			result = errs.New(errs.KindInternal, "Agent log event queue overflow")
			cancel()
		}
	}
	if sourceErr := <-sourceDone; result == nil {
		result = sourceErr
	}
	close(events)
	done <- result
}

func (manager *logManager) release(requestID string) {
	manager.mu.Lock()
	delete(manager.active, requestID)
	manager.mu.Unlock()
}

func (manager *logManager) send(ctx context.Context, message *agentpb.AgentMessage) bool {
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
