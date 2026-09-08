package agentchannel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Rationale: ADR 0059 distinguishes pre-ready Docker availability from
// invalid source state, while every post-ready failure only closes SSE.
func TestRecordLogEndPreservesPreReadyClassification(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		reason agentpb.LogEndReason
		kind   errs.Kind
	}{
		{name: "availability", reason: agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED, kind: errs.KindStorageUnavailable},
		{name: "source", reason: agentpb.LogEndReason_LOG_END_REASON_SOURCE_FAILED, kind: errs.KindInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry()
			session, err := registry.Open(context.Background(), "agent-1", 1)
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer session.Close()
			result := make(chan error, 1)
			go func() {
				_, openErr := registry.OpenLogs(context.Background(), testLogScope(), nil, 0, false)
				result <- openErr
			}()
			command := <-session.logMessages()
			command.result <- nil
			if err := session.RecordLogEnd(&agentpb.LogEnd{
				RequestId: command.message.GetLogSubscribe().GetRequestId(), Reason: test.reason,
			}); err != nil {
				t.Fatalf("RecordLogEnd() error = %v", err)
			}
			if openErr := <-result; !errors.Is(openErr, errs.New(test.kind, "")) {
				t.Fatalf("OpenLogs() error = %v, want %v", openErr, test.kind)
			}
		})
	}
}

// Rationale: after readiness both availability and source failures are stream
// termination only and must not synthesize a second public error contract.
func TestRecordLogEndAfterReadyOnlyClosesEvents(t *testing.T) {
	t.Parallel()

	for _, reason := range []agentpb.LogEndReason{
		agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED,
		agentpb.LogEndReason_LOG_END_REASON_SOURCE_FAILED,
	} {
		t.Run(reason.String(), func(t *testing.T) {
			registry := NewRegistry()
			session, err := registry.Open(context.Background(), "agent-1", 1)
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer session.Close()
			result := make(chan *LogSubscription, 1)
			go func() {
				subscription, openErr := registry.OpenLogs(context.Background(), testLogScope(), nil, 0, false)
				if openErr != nil {
					t.Errorf("OpenLogs() error = %v", openErr)
				}
				result <- subscription
			}()
			command := <-session.logMessages()
			command.result <- nil
			requestID := command.message.GetLogSubscribe().GetRequestId()
			if err := session.RecordLogReady(&agentpb.LogReady{RequestId: requestID}); err != nil {
				t.Fatalf("RecordLogReady() error = %v", err)
			}
			subscription := <-result
			if err := session.RecordLogEnd(&agentpb.LogEnd{RequestId: requestID, Reason: reason}); err != nil {
				t.Fatalf("RecordLogEnd() error = %v", err)
			}
			if _, open := <-subscription.Events(); open {
				t.Fatal("Events() remained open after terminal LogEnd")
			}
		})
	}
}

// Rationale: cleanup timeout before the unbuffered cancel handoff must retain
// durable in-session ownership until acknowledged delivery, allowing repeated
// subscriptions to reuse all eight bounded slots without Agent orphans.
func TestCancelLogsRetainsOwnershipUntilDeliveryAndRepeatedlyReleasesSlot(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), "agent-1", 1)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()
	for iteration := range maximumLogSubscriptions * 2 {
		opened := make(chan *LogSubscription, 1)
		openErrors := make(chan error, 1)
		go func() {
			subscription, openErr := registry.OpenLogs(context.Background(), testLogScope(), nil, 0, true)
			opened <- subscription
			openErrors <- openErr
		}()
		subscribe := <-session.logMessages()
		subscribe.result <- nil
		requestID := subscribe.message.GetLogSubscribe().GetRequestId()
		if err := session.RecordLogReady(&agentpb.LogReady{RequestId: requestID}); err != nil {
			t.Fatalf("iteration %d RecordLogReady() error = %v", iteration, err)
		}
		subscription := <-opened
		if openErr := <-openErrors; openErr != nil {
			t.Fatalf("iteration %d OpenLogs() error = %v", iteration, openErr)
		}

		cleanup, expireCleanup := context.WithCancel(context.Background())
		expireCleanup()
		subscription.Cancel(cleanup)
		registry.mu.Lock()
		retained := registry.logs[requestID] == subscription
		registry.mu.Unlock()
		if !retained {
			t.Fatalf("iteration %d forgot cancellation before handoff", iteration)
		}

		cancel := <-session.logMessages()
		if cancel.message.GetLogCancel().GetRequestId() != requestID {
			t.Fatalf("iteration %d LogCancel = %#v, want %q", iteration, cancel.message.GetLogCancel(), requestID)
		}
		cancel.result <- nil
		select {
		case <-subscription.cancelDone:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d cancellation was not acknowledged", iteration)
		}
	}
	registry.mu.Lock()
	active := len(registry.logs)
	registry.mu.Unlock()
	if active != 0 {
		t.Fatalf("active log slots after repeated cancellation = %d, want 0", active)
	}
}

// Rationale: after the subscribe handoff, HTTP cancellation must not wait for
// a stalled stream.Send result, while the session still owns eventual cleanup.
func TestOpenLogsCancellationAfterHandoffDoesNotWaitForSendResult(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), "agent-1", 1)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()

	ctx, cancel := context.WithCancel(context.Background())
	openResult := make(chan error, 1)
	go func() {
		_, openErr := registry.OpenLogs(ctx, testLogScope(), []LogTarget{testLogTarget()}, 0, true)
		openResult <- openErr
	}()
	subscribe := <-session.logMessages()
	cancel()
	select {
	case openErr := <-openResult:
		if !errors.Is(openErr, context.Canceled) {
			t.Fatalf("OpenLogs() error = %v, want context canceled", openErr)
		}
	case <-time.After(time.Second):
		t.Fatal("OpenLogs() waited for the stalled subscribe send result")
	}

	requestID := subscribe.message.GetLogSubscribe().GetRequestId()
	registry.mu.Lock()
	subscription := registry.logs[requestID]
	if subscription == nil || subscription.canceling {
		registry.mu.Unlock()
		t.Fatalf("subscription after caller cancellation = %#v, want retained and not yet canceling", subscription)
	}
	registry.mu.Unlock()

	subscribe.result <- nil
	cancelCommand := <-session.logMessages()
	if got := cancelCommand.message.GetLogCancel().GetRequestId(); got != requestID {
		t.Fatalf("LogCancel request id = %q, want %q", got, requestID)
	}
	registry.mu.Lock()
	if registry.logs[requestID] != subscription || !subscription.canceling {
		registry.mu.Unlock()
		t.Fatal("subscription ownership was not retained while LogCancel delivery was pending")
	}
	registry.mu.Unlock()
	cancelCommand.result <- nil
	select {
	case <-subscription.cancelDone:
	case <-time.After(time.Second):
		t.Fatal("LogCancel delivery did not release the subscription")
	}
}

// Rationale: queue overflow has initiated an Agent cancellation, so its slot
// remains owned until that LogCancel is delivered rather than being reused early.
func TestRecordLogEventOverflowRetainsSlotUntilCancelDelivery(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), "agent-1", 1)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()

	subscriptions := make([]*LogSubscription, maximumLogSubscriptions)
	for index := range subscriptions {
		subscriptions[index] = openReadyLogSubscription(t, registry, session)
	}
	overflowed := subscriptions[0]
	for index := range maximumQueuedLogEvents {
		overflow, recordErr := session.RecordLogEvent(testLogEvent(overflowed.ID, int64(index+1)))
		if recordErr != nil || overflow {
			t.Fatalf("RecordLogEvent(%d) = (%v, %v), want (false, nil)", index+1, overflow, recordErr)
		}
	}
	overflow, recordErr := session.RecordLogEvent(testLogEvent(overflowed.ID, maximumQueuedLogEvents+1))
	if recordErr != nil || !overflow {
		t.Fatalf("overflow RecordLogEvent() = (%v, %v), want (true, nil)", overflow, recordErr)
	}

	registry.mu.Lock()
	if registry.logs[overflowed.ID] != overflowed || !overflowed.canceling ||
		len(registry.logs) != maximumLogSubscriptions {
		registry.mu.Unlock()
		t.Fatal("overflow did not retain the canceling subscription and its slot")
	}
	registry.mu.Unlock()
	if _, openErr := registry.OpenLogs(context.Background(), testLogScope(), []LogTarget{testLogTarget()}, 0, true); !errors.Is(
		openErr,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("OpenLogs() while overflow cancellation is pending = %v, want state conflict", openErr)
	}
	if err := session.RecordLogCancelDelivered(overflowed.ID); err != nil {
		t.Fatalf("RecordLogCancelDelivered() error = %v", err)
	}
	select {
	case <-overflowed.cancelDone:
	case <-time.After(time.Second):
		t.Fatal("successful LogCancel delivery did not release the overflowed subscription")
	}
	if replacement := openReadyLogSubscription(t, registry, session); replacement == nil {
		t.Fatal("replacement subscription is nil")
	}
}

func openReadyLogSubscription(t *testing.T, registry *Registry, session *Session) *LogSubscription {
	t.Helper()
	result := make(chan *LogSubscription, 1)
	errResult := make(chan error, 1)
	go func() {
		subscription, err := registry.OpenLogs(
			context.Background(),
			testLogScope(),
			[]LogTarget{testLogTarget()},
			0,
			true,
		)
		result <- subscription
		errResult <- err
	}()
	command := <-session.logMessages()
	command.result <- nil
	requestID := command.message.GetLogSubscribe().GetRequestId()
	if err := session.RecordLogReady(&agentpb.LogReady{RequestId: requestID}); err != nil {
		t.Fatalf("RecordLogReady() error = %v", err)
	}
	if openErr := <-errResult; openErr != nil {
		t.Fatalf("OpenLogs() error = %v", openErr)
	}
	return <-result
}

func testLogTarget() LogTarget {
	return LogTarget{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceID:     "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceName:   "api",
		ReleaseID:     "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
}

func testLogEvent(requestID string, sequence int64) *agentpb.LogEvent {
	return &agentpb.LogEvent{
		RequestId:     requestID,
		EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceId:     "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceName:   "api",
		ReleaseId:     "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ContainerId:   "groundplane-api-blue-01",
		ContainerName: "groundplane-api-blue-01",
		Slot:          agentpb.LogSlot_LOG_SLOT_BLUE,
		Stream:        agentpb.LogStream_LOG_STREAM_STDOUT,
		Timestamp:     timestamppb.New(time.Unix(sequence, 0).UTC()),
		Line:          "line",
	}
}

func testLogScope() LogScope {
	return LogScope{EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"}
}
