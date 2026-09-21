package logstream

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
			manager := New(logReaderStub{err: test.err})
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

	manager := New(logReaderStub{sources: emptyLogSources{}})
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

// QA: LOG-01, LOG-02; bounded Agent backpressure and cancellation only, not
// public HTTP/SSE or a real Docker source.
// Rationale: a valid tail may exceed the internal queue bound. The Agent must
// backpressure that burst without turning normal replay into a source failure,
// while cancellation must still join the producer, close sources, and free its slot.
func TestLogManagerBackpressuresReplayLargerThanQueueUntilCancellation(t *testing.T) {
	t.Parallel()

	const records = maxQueuedLogEvents + 72
	sources := &burstLogSources{
		exited: make(chan struct{}), allSent: make(chan struct{}), records: records,
	}
	manager := New(logReaderStub{sources: sources})
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
	producerDeadline := time.Now().Add(time.Second)
	for sources.sent.Load() <= maxQueuedLogEvents {
		if time.Now().After(producerDeadline) {
			t.Fatalf(
				"producer submitted %d records before output drain, want more than %d",
				sources.sent.Load(), maxQueuedLogEvents,
			)
		}
		time.Sleep(time.Millisecond)
	}
	for sequence := range records {
		select {
		case message := <-manager.Outputs():
			event := message.GetLogEvent()
			if event == nil || event.GetLine() != string(rune(sequence)) {
				t.Fatalf("message %d = %#v, want matching LogEvent", sequence+1, message)
			}
		case <-time.After(time.Second):
			t.Fatalf("log manager stopped after %d/%d replay records", sequence, records)
		}
	}
	select {
	case <-sources.allSent:
	case <-time.After(time.Second):
		t.Fatal("producer did not finish the bounded replay")
	}
	if sources.sent.Load() != records {
		t.Fatalf("producer sent %d records, want %d", sources.sent.Load(), records)
	}
	select {
	case message := <-manager.Outputs():
		t.Fatalf("follow stream ended after replay: %#v", message)
	case <-time.After(25 * time.Millisecond):
	}

	cancel()
	select {
	case <-sources.exited:
	case <-time.After(time.Second):
		t.Fatal("cancelled producer did not exit")
	}
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.Lock()
		active := len(manager.active)
		manager.mu.Unlock()
		if active == 0 && sources.closes.Load() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancel cleanup: active=%d close calls=%d", active, sources.closes.Load())
		}
		time.Sleep(time.Millisecond)
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
	allSent chan struct{}
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
	close(sources.allSent)
	<-ctx.Done()
	return ctx.Err()
}

func (sources *burstLogSources) Close() error {
	sources.closes.Add(1)
	return nil
}

var _ io.Closer = emptyLogSources{}
