package agent

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

// QA: LOG-01; injected source-open classification only, not Docker selection or public HTTP/SSE status.
// Rationale: only typed Docker availability may become a pre-ready 503;
// malformed managed source state must remain the distinct internal failure path.
func TestLogManagerClassifiesOpenFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		err    error
		reason agentpb.LogEndReason
	}{
		{name: "availability", err: errs.New(errs.KindStorageUnavailable, "Docker unavailable"), reason: agentpb.LogEndReason_LOG_END_REASON_AVAILABILITY_FAILED},
		{name: "source", err: errs.New(errs.KindInternal, "managed source invalid"), reason: agentpb.LogEndReason_LOG_END_REASON_SOURCE_FAILED},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newLogManager(logReaderStub{err: test.err})
			manager.Subscribe(context.Background(), validAgentLogSubscribe())
			select {
			case message := <-manager.Outputs():
				if end := message.GetLogEnd(); end == nil || end.GetReason() != test.reason {
					t.Fatalf("LogEnd = %#v, want reason %s", end, test.reason)
				}
			case <-time.After(time.Second):
				t.Fatal("log manager did not classify Open failure")
			}
		})
	}
}

// QA: LOG-01; fake zero-source Agent handshake only, not Controller SSE headers or fixed source selection.
// Rationale: a valid zero-source subscription still crosses readiness and
// completes normally, allowing the Controller to commit 200 SSE and return EOF.
func TestLogManagerReadiesAndCompletesZeroSources(t *testing.T) {
	t.Parallel()

	manager := newLogManager(logReaderStub{sources: emptyLogSources{}})
	manager.Subscribe(context.Background(), validAgentLogSubscribe())
	for _, want := range []string{"ready", "complete"} {
		select {
		case message := <-manager.Outputs():
			if want == "ready" && message.GetLogReady() == nil {
				t.Fatalf("first message = %#v, want LogReady", message)
			}
			if want == "complete" {
				end := message.GetLogEnd()
				if end == nil || end.GetReason() != agentpb.LogEndReason_LOG_END_REASON_COMPLETE {
					t.Fatalf("second message = %#v, want complete LogEnd", message)
				}
			}
		case <-time.After(time.Second):
			t.Fatalf("log manager did not send %s", want)
		}
	}
}

// QA: LOG-02, TASK-07; local queue overflow and cleanup only, not client reconnect or process memory measurement.
// Rationale: the 129th undrained record must overflow the exact 128-record
// ownership bound, cancel and join the producer, close sources, and free the Agent slot.
func TestLogManagerTerminatesUndrainedQueueOverflow(t *testing.T) {
	t.Parallel()

	const expectedMaximumQueuedLogEvents = 128
	sources := &burstLogSources{exited: make(chan struct{}), records: expectedMaximumQueuedLogEvents + 1}
	manager := newLogManager(logReaderStub{sources: sources})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.Subscribe(ctx, validAgentLogSubscribe())
	select {
	case message := <-manager.Outputs():
		if message.GetLogReady() == nil {
			t.Fatalf("first message = %#v, want LogReady", message)
		}
	case <-time.After(time.Second):
		t.Fatal("log manager did not become ready")
	}
	select {
	case <-sources.exited:
	case <-time.After(time.Second):
		t.Fatal("129-record producer remained blocked after queue overflow")
	}
	if sources.sent.Load() != expectedMaximumQueuedLogEvents+1 {
		t.Fatalf("producer sent %d records, want %d", sources.sent.Load(), expectedMaximumQueuedLogEvents+1)
	}

	deadline := time.After(time.Second)
	for {
		select {
		case message := <-manager.Outputs():
			end := message.GetLogEnd()
			if end == nil {
				continue
			}
			if end.GetReason() != agentpb.LogEndReason_LOG_END_REASON_SOURCE_FAILED {
				t.Fatalf("LogEnd reason = %s, want source failed", end.GetReason())
			}
			if sources.closes.Load() != 1 {
				t.Fatalf("source Close() calls = %d, want 1", sources.closes.Load())
			}
			manager.mu.Lock()
			active := len(manager.active)
			manager.mu.Unlock()
			if active != 0 {
				t.Fatalf("active Agent log slots = %d, want 0", active)
			}
			return
		case <-deadline:
			t.Fatal("queue overflow did not terminate the subscription")
		}
	}
}

type logReaderStub struct {
	sources agentprotocol.LogSourceSet
	err     error
}

func (reader logReaderStub) Open(context.Context, *agentpb.LogSubscribe) (agentprotocol.LogSourceSet, error) {
	return reader.sources, reader.err
}

type emptyLogSources struct{}

func (emptyLogSources) Run(context.Context, chan<- *agentpb.LogEvent) error { return nil }
func (emptyLogSources) Close() error                                        { return nil }

func validAgentLogSubscribe() *agentpb.LogSubscribe {
	return &agentpb.LogSubscribe{RequestId: "request-1"}
}

type burstLogSources struct {
	exited  chan struct{}
	records int
	sent    atomic.Int32
	closes  atomic.Int32
}

func (sources *burstLogSources) Run(ctx context.Context, output chan<- *agentpb.LogEvent) error {
	defer close(sources.exited)
	for sequence := range sources.records {
		select {
		case output <- &agentpb.LogEvent{RequestId: "request-1", Line: string(rune(sequence))}:
			sources.sent.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// The final send only hands off the record. Wait for overflow cancellation
	// so the test cannot drain output before the collector checks that record.
	<-ctx.Done()
	return ctx.Err()
}

func (sources *burstLogSources) Close() error {
	sources.closes.Add(1)
	return nil
}

var _ io.Closer = emptyLogSources{}
