package agentchannel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	maximumLogTargets       = 128
	maximumLogSubscriptions = 8
	maximumQueuedLogEvents  = 128
	maximumPublicLogLine    = 32 * 1024
)

type LogScope struct {
	EnvironmentID string
	ServiceID     string
}

type LogTarget struct {
	EnvironmentID string
	ServiceID     string
	ServiceName   string
	ReleaseID     string
}

type LogSubscription struct {
	ID      string
	Tail    uint32
	Follow  bool
	scope   LogScope
	targets []LogTarget

	registry *Registry
	state    *sessionState
	ready    chan error
	events   chan *agentpb.LogEvent
	mu       sync.Mutex
	isReady  bool
	closed   bool
}

func (subscription *LogSubscription) Events() <-chan *agentpb.LogEvent {
	return subscription.events
}

func (subscription *LogSubscription) Scope() LogScope {
	return subscription.scope
}

func (subscription *LogSubscription) Targets() []LogTarget {
	return append([]LogTarget(nil), subscription.targets...)
}

func (subscription *LogSubscription) Cancel(ctx context.Context) {
	if subscription != nil && subscription.registry != nil {
		subscription.registry.CancelLogs(ctx, subscription.ID)
	}
}

func (registry *Registry) OpenLogs(
	ctx context.Context,
	scope LogScope,
	targets []LogTarget,
	tail uint32,
	follow bool,
) (*LogSubscription, error) {
	if ctx == nil || scope.EnvironmentID == "" || len(targets) > maximumLogTargets || tail > 1000 {
		return nil, errs.New(errs.KindValidationFailed, "log subscription is invalid")
	}
	if ids.Validate(ids.KindEnvironment, scope.EnvironmentID) != nil ||
		(scope.ServiceID != "" && ids.Validate(ids.KindService, scope.ServiceID) != nil) {
		return nil, errs.New(errs.KindValidationFailed, "log subscription scope is invalid")
	}
	seenServices := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target.EnvironmentID != scope.EnvironmentID || ids.Validate(ids.KindService, target.ServiceID) != nil ||
			ids.Validate(ids.KindDeployment, target.ReleaseID) != nil || target.ServiceName == "" {
			return nil, errs.New(errs.KindValidationFailed, "log subscription target is invalid")
		}
		if scope.ServiceID != "" && target.ServiceID != scope.ServiceID {
			return nil, errs.New(errs.KindValidationFailed, "log subscription target is outside its scope")
		}
		if _, exists := seenServices[target.ServiceID]; exists {
			return nil, errs.New(errs.KindValidationFailed, "log subscription has a duplicate Service target")
		}
		seenServices[target.ServiceID] = struct{}{}
	}
	requestID, err := newLogRequestID()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}

	registry.mu.Lock()
	if len(registry.logs) >= maximumLogSubscriptions {
		registry.mu.Unlock()
		return nil, errs.New(errs.KindStateConflict, "maximum active log subscriptions reached")
	}
	var state *sessionState
	for _, candidate := range registry.agents {
		if candidate.online && !candidate.revoked {
			state = candidate
			break
		}
	}
	if state == nil {
		registry.mu.Unlock()
		return nil, errs.New(errs.KindStorageUnavailable, "Agent is unavailable for logs")
	}
	ownedTargets := append([]LogTarget(nil), targets...)
	subscription := &LogSubscription{
		ID: requestID, scope: scope, targets: ownedTargets, Tail: tail, Follow: follow,
		registry: registry, state: state, ready: make(chan error, 1),
		events: make(chan *agentpb.LogEvent, maximumQueuedLogEvents),
	}
	registry.logs[requestID] = subscription
	commands := state.logCommands
	done := state.done
	registry.mu.Unlock()

	protobufTargets := make([]*agentpb.LogTarget, 0, len(ownedTargets))
	for _, target := range ownedTargets {
		protobufTargets = append(protobufTargets, &agentpb.LogTarget{
			EnvironmentId: target.EnvironmentID,
			ServiceId:     target.ServiceID,
			ServiceName:   target.ServiceName,
			ReleaseId:     target.ReleaseID,
		})
	}
	command := logCommand{
		message: &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_LogSubscribe{LogSubscribe: &agentpb.LogSubscribe{
			RequestId: requestID, Targets: protobufTargets, Tail: tail, Follow: follow,
		}}},
		result: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		registry.dropLog(requestID)
		return nil, ctx.Err()
	case <-done:
		registry.dropLog(requestID)
		return nil, errs.New(errs.KindStorageUnavailable, "Agent log session ended before delivery")
	case commands <- command:
	}
	select {
	case <-ctx.Done():
		registry.dropLog(requestID)
		return nil, ctx.Err()
	case <-done:
		registry.dropLog(requestID)
		return nil, errs.New(errs.KindStorageUnavailable, "Agent log session ended during delivery")
	case err := <-command.result:
		if err != nil {
			registry.dropLog(requestID)
			return nil, err
		}
	}
	select {
	case <-ctx.Done():
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		registry.CancelLogs(cleanup, requestID)
		cancel()
		return nil, ctx.Err()
	case err := <-subscription.ready:
		if err != nil {
			return nil, err
		}
		return subscription, nil
	}
}

func (registry *Registry) CancelLogs(ctx context.Context, requestID string) {
	registry.mu.Lock()
	subscription := registry.logs[requestID]
	if subscription == nil {
		registry.mu.Unlock()
		return
	}
	delete(registry.logs, requestID)
	subscription.finishLocked(nil)
	commands := subscription.state.logCommands
	done := subscription.state.done
	registry.mu.Unlock()

	command := logCommand{
		message: &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_LogCancel{LogCancel: &agentpb.LogCancel{RequestId: requestID}}},
		result:  make(chan error, 1),
	}
	select {
	case commands <- command:
	case <-ctx.Done():
	case <-done:
		return
	}
}

func (session *Session) RecordLogReady(ready *agentpb.LogReady) error {
	if ready == nil || ready.GetRequestId() == "" {
		return errs.New(errs.KindValidationFailed, "Agent LogReady is invalid")
	}
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	subscription := session.registry.logs[ready.GetRequestId()]
	if subscription == nil {
		return nil
	}
	if subscription.state != session.state || subscription.closed || subscription.isReady {
		return errs.New(errs.KindStateConflict, "Agent LogReady does not match an active subscription")
	}
	subscription.isReady = true
	subscription.ready <- nil
	return nil
}

func (session *Session) RecordLogEvent(event *agentpb.LogEvent) (bool, error) {
	if event == nil || event.GetRequestId() == "" || event.GetTimestamp() == nil ||
		event.GetTimestamp().CheckValid() != nil || len(event.GetLine()) > maximumPublicLogLine ||
		event.GetContainerId() == "" || event.GetContainerName() == "" ||
		(event.GetSlot() != agentpb.LogSlot_LOG_SLOT_BLUE &&
			event.GetSlot() != agentpb.LogSlot_LOG_SLOT_GREEN &&
			event.GetSlot() != agentpb.LogSlot_LOG_SLOT_SINGLETON) ||
		(event.GetStream() != agentpb.LogStream_LOG_STREAM_STDOUT &&
			event.GetStream() != agentpb.LogStream_LOG_STREAM_STDERR) {
		return false, errs.New(errs.KindValidationFailed, "Agent LogEvent is invalid")
	}
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	subscription := session.registry.logs[event.GetRequestId()]
	if subscription == nil {
		return false, nil
	}
	if subscription.state != session.state || subscription.closed || !subscription.isReady {
		return false, errs.New(errs.KindStateConflict, "Agent LogEvent does not match a ready subscription")
	}
	if !subscription.accepts(event) {
		return false, errs.New(errs.KindStateConflict, "Agent LogEvent is outside the fixed subscription sources")
	}
	select {
	case subscription.events <- event:
		return false, nil
	default:
		delete(session.registry.logs, subscription.ID)
		subscription.finishLocked(nil)
		return true, nil
	}
}

func (session *Session) RecordLogEnd(end *agentpb.LogEnd) error {
	if end == nil || end.GetRequestId() == "" || end.GetReason() == agentpb.LogEndReason_LOG_END_REASON_UNSPECIFIED {
		return errs.New(errs.KindValidationFailed, "Agent LogEnd is invalid")
	}
	session.registry.mu.Lock()
	defer session.registry.mu.Unlock()
	subscription := session.registry.logs[end.GetRequestId()]
	if subscription == nil {
		return nil
	}
	if subscription.state != session.state || subscription.closed {
		return errs.New(errs.KindStateConflict, "Agent LogEnd does not match an active subscription")
	}
	delete(session.registry.logs, subscription.ID)
	var err error
	if !subscription.isReady {
		err = errs.New(errs.KindStorageUnavailable, "Agent could not initialize log sources")
	}
	subscription.finishLocked(err)
	return nil
}

func (registry *Registry) failLogsLocked(state *sessionState, err error) {
	for requestID, subscription := range registry.logs {
		if subscription.state == state {
			delete(registry.logs, requestID)
			subscription.finishLocked(err)
		}
	}
}

func (subscription *LogSubscription) finishLocked(err error) {
	if subscription.closed {
		return
	}
	subscription.closed = true
	if !subscription.isReady {
		subscription.ready <- err
	}
	close(subscription.events)
}

func (subscription *LogSubscription) accepts(event *agentpb.LogEvent) bool {
	for _, target := range subscription.targets {
		if event.GetEnvironmentId() == target.EnvironmentID && event.GetServiceId() == target.ServiceID &&
			event.GetServiceName() == target.ServiceName && event.GetReleaseId() == target.ReleaseID {
			return true
		}
	}
	return false
}

func (registry *Registry) dropLog(requestID string) {
	registry.mu.Lock()
	if subscription := registry.logs[requestID]; subscription != nil {
		delete(registry.logs, requestID)
		subscription.finishLocked(nil)
	}
	registry.mu.Unlock()
}

func newLogRequestID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
