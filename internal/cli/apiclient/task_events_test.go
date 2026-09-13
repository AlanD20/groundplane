package apiclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a finite CLI process needs to distinguish authoritative terminal
// closure from a live disconnect and resume the latter with its last sequence.
// QA: TASK-06; local reconnect/parser behavior, not durable compaction or live drain.
func TestStreamTaskEventsReconnectsWithGeneratedResumeHeader(t *testing.T) {
	t.Parallel()
	taskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	var streamRequests atomic.Int32
	var showRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v1/tasks/" + taskID + "/events":
			attempt := streamRequests.Add(1)
			if request.Header.Get("Accept") != "text/event-stream" {
				t.Errorf("Accept = %q", request.Header.Get("Accept"))
			}
			wantResume := ""
			if attempt == 2 {
				wantResume = "1"
			}
			if got := request.Header.Get("Last-Event-ID"); got != wantResume {
				t.Errorf("stream %d Last-Event-ID = %q, want %q", attempt, got, wantResume)
			}
			writer.Header().Set("Content-Type", "text/event-stream")
			sequence := attempt
			_, _ = fmt.Fprintf(
				writer,
				"id: %d\ndata: {\"sequence\":%d,\"step_id\":\"step_01ARZ3NDEKTSV4RRFFQ69G5FAV\","+
					"\"state\":\"running\",\"attempt\":1,\"ordinal\":%d,"+
					"\"received_at\":\"2026-08-23T04:30:00Z\"}\n\n",
				sequence,
				sequence,
				sequence,
			)
		case "/api/v1/tasks/" + taskID:
			status := "running"
			if showRequests.Add(1) == 2 {
				status = "completed"
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(
				writer,
				`{"id":%q,"operation_id":"op_01ARZ3NDEKTSV4RRFFQ69G5FAV","type":"run",`+
					`"target":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":%q}`,
				taskID,
				status,
			)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	var sequences []uint64
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := New(server.URL).StreamTaskEvents(ctx, taskID, func(event apiTypes.TaskEvent) error {
		sequences = append(sequences, event.Sequence)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamTaskEvents() error = %v", err)
	}
	if fmt.Sprint(sequences) != "[1 2]" || streamRequests.Load() != 2 || showRequests.Load() != 2 {
		t.Fatalf(
			"StreamTaskEvents() sequences=%v streams=%d shows=%d",
			sequences,
			streamRequests.Load(),
			showRequests.Load(),
		)
	}
}

// Rationale: at-least-once delivery across reconnects must remain exactly once
// to one live CLI callback, while a genuine sequence gap fails closed.
// QA: TASK-06; local reconnect/parser behavior, not durable compaction or live drain.
func TestConsumeTaskEventStreamDeduplicatesReplayAndRejectsGap(t *testing.T) {
	t.Parallel()
	event := func(sequence int) string {
		return fmt.Sprintf(
			"id: %d\ndata: {\"sequence\":%d,\"step_id\":\"step_1\",\"state\":\"running\","+
				"\"attempt\":1,\"ordinal\":%d,\"received_at\":\"2026-08-23T04:30:00Z\"}\n\n",
			sequence,
			sequence,
			sequence,
		)
	}
	var sequences []uint64
	last, err := consumeTaskEventStream(
		context.Background(),
		"/tasks/task_1/events",
		strings.NewReader(event(1)+event(1)+event(2)),
		0,
		func(event apiTypes.TaskEvent) error {
			sequences = append(sequences, event.Sequence)
			return nil
		},
	)
	if err != nil || last != 2 || fmt.Sprint(sequences) != "[1 2]" {
		t.Fatalf("consume replay = last %d, sequences %v, error %v", last, sequences, err)
	}
	if _, err := consumeTaskEventStream(
		context.Background(),
		"/tasks/task_1/events",
		strings.NewReader(event(2)),
		0,
		func(apiTypes.TaskEvent) error { return nil },
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("consume gap error = %v, want internal", err)
	}
}

// Rationale: a connection ending inside a frame has not delivered that event;
// the retryable classification is what makes the next request resume safely.
// QA: TASK-06; local reconnect/parser behavior, not durable compaction or live drain.
func TestConsumeTaskEventStreamTreatsPartialFrameAsRetryable(t *testing.T) {
	t.Parallel()
	last, err := consumeTaskEventStream(
		context.Background(),
		"/tasks/task_1/events",
		strings.NewReader("id: 1\ndata: {\"sequence\":1}"),
		0,
		func(apiTypes.TaskEvent) error {
			t.Error("incomplete frame reached callback")
			return nil
		},
	)
	if !errs.IsRetryable(err) || last != 0 {
		t.Fatalf("partial frame = last %d, error %v; want unchanged cursor and retryable", last, err)
	}
}
