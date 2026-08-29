package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

const maxLogSubscriptions = 8

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
		manager.send(parent, logEndMessage(request.GetRequestId(), agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED))
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
	defer func() {
		manager.mu.Lock()
		delete(manager.active, requestID)
		manager.mu.Unlock()
	}()

	sources, err := manager.reader.Open(ctx, request)
	if err != nil {
		manager.send(ctx, logEndMessage(requestID, agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED))
		return
	}
	defer sources.Close()

	if !manager.send(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_LogReady{LogReady: &agentpb.LogReady{RequestId: requestID}}}) {
		return
	}

	events := make(chan *agentpb.LogEvent, 128)
	done := make(chan error, 1)
	go func() {
		err := sources.Run(ctx, events)
		close(events)
		done <- err
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case event, open := <-events:
			if !open {
				err := <-done
				reason := agentpb.LogEndReason_LOG_END_REASON_COMPLETE
				if err != nil {
					reason = agentpb.LogEndReason_LOG_END_REASON_SOURCE_FAILED
				}
				manager.send(ctx, logEndMessage(requestID, reason))
				return
			}
			if event != nil && !manager.send(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_LogEvent{LogEvent: event}}) {
				return
			}
		}
	}
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
