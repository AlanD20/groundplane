package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskEventRouteTestTaskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// QA: TASK-06, UI-03; header parser only, not snapshot validation or resumed event delivery.
// Rationale: resume input must be one canonical uint64, never an ambiguous or normalized value.
func TestParseLastTaskEventIDAcceptsOnlyCanonicalSingleValues(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   uint64
		valid  bool
	}{
		{name: "absent", valid: true},
		{name: "zero", values: []string{"0"}, valid: true},
		{name: "positive", values: []string{"18446744073709551615"}, want: ^uint64(0), valid: true},
		{name: "empty", values: []string{""}},
		{name: "signed", values: []string{"+1"}},
		{name: "negative", values: []string{"-1"}},
		{name: "whitespace", values: []string{" 1"}},
		{name: "non_ascii", values: []string{"١"}},
		{name: "leading_zero", values: []string{"01"}},
		{name: "overflow", values: []string{"18446744073709551616"}},
		{name: "too_long", values: []string{"100000000000000000000"}},
		{name: "multiple", values: []string{"1", "2"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := make(http.Header)
			for _, value := range test.values {
				header.Add("Last-Event-ID", value)
			}
			got, err := parseLastTaskEventID(header)
			if test.valid {
				if err != nil || got != test.want {
					t.Fatalf("parseLastTaskEventID() = %d, %v, want %d", got, err, test.want)
				}
				return
			}
			if !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
				t.Fatalf("parseLastTaskEventID() error = %v, want malformed request", err)
			}
		})
	}
}

// QA: TASK-06/07; local HTTP with an injected journal, not persistence, replay filtering or terminal drain.
// Rationale: the route must forward the exact resume point and emit only the
// closed public event shape, with sequence as its SSE id.
func TestTaskEventRouteStreamsTypedSequenceFrames(t *testing.T) {
	receivedAt := time.Date(2026, time.August, 23, 4, 30, 0, 0, time.UTC)
	opener := &fakeTaskEventStreamOpener{runner: fakeTaskEventRunner{events: []etcd.TaskEventRecord{{
		Sequence: 4,
		Identity: etcd.TaskEventIdentity{
			TaskID: taskEventRouteTestTaskID, StepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", Attempt: 2, Ordinal: 7,
		},
		State: etcd.TaskEventStateRunning, ReceivedAt: receivedAt,
	}}}}
	server := taskEventRouteTestServer(t, opener, io.Discard)
	response := requestTaskEventStream(t, server.URL, "3")
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read Task event response: %v", err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("Task event response = %d %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	want := "id: 4\ndata: {\"sequence\":4,\"step_id\":\"step_01ARZ3NDEKTSV4RRFFQ69G5FAV\"," +
		"\"state\":\"running\",\"attempt\":2,\"ordinal\":7,\"received_at\":\"2026-08-23T04:30:00Z\"}\n\n"
	if string(body) != want {
		t.Fatalf("Task event body = %q, want %q", body, want)
	}
	if opener.taskID != taskEventRouteTestTaskID || opener.after != 3 {
		t.Fatalf("OpenTaskEventStream() received task=%q after=%d", opener.taskID, opener.after)
	}
}

// QA: TASK-06, UI-03; injected opener rejection through HTTP, not actual journal-boundary validation.
// Rationale: a rejected resume point must remain validation.failed/400 before
// SSE headers commit, preserving an ordinary problem response.
func TestTaskEventRouteMapsResumeRejectionBeforeSSEHeaders(t *testing.T) {
	opener := &fakeTaskEventStreamOpener{err: errs.New(
		errs.KindMalformedRequest,
		"Last-Event-ID is ahead of the Task journal",
	)}
	server := taskEventRouteTestServer(t, opener, io.Discard)
	response := requestTaskEventStream(t, server.URL, "9")
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest ||
		!strings.HasPrefix(response.Header.Get("Content-Type"), "application/problem+json") {
		t.Fatalf("ahead response = %d %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	var problem errs.Problem
	if err := json.NewDecoder(response.Body).Decode(&problem); err != nil {
		t.Fatalf("decode resume rejection: %v", err)
	}
	if problem.Type != "about:blank" || problem.Code != "validation.failed" || problem.Status != 400 ||
		opener.taskID != taskEventRouteTestTaskID || opener.after != 9 {
		t.Fatalf("resume problem = %#v, opener = %#v", problem, opener)
	}
}

// QA: TASK-06/07; injected post-frame failure over local HTTP, not a real corrupt journal or reconnect.
// Rationale: a failure after output must preserve the emitted frame, close the
// stream without inventing another event, and log only a safe failure class.
func TestTaskEventRouteDisconnectsSafelyAfterHeaders(t *testing.T) {
	var logs bytes.Buffer
	opener := &fakeTaskEventStreamOpener{runner: fakeTaskEventRunner{
		events: []etcd.TaskEventRecord{{
			Sequence: 1,
			Identity: etcd.TaskEventIdentity{
				TaskID: taskEventRouteTestTaskID, StepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", Attempt: 1, Ordinal: 1,
			},
			State:      etcd.TaskEventStateRunning,
			ReceivedAt: time.Date(2026, time.August, 23, 4, 30, 0, 0, time.UTC),
		}},
		err: errs.New(errs.KindInternal, "secret Agent diagnostic"),
	}}
	server := taskEventRouteTestServer(t, opener, &logs)
	response := requestTaskEventStream(t, server.URL, "")
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read Task event response: %v", err)
	}
	want := "id: 1\ndata: {\"sequence\":1,\"step_id\":\"step_01ARZ3NDEKTSV4RRFFQ69G5FAV\"," +
		"\"state\":\"running\",\"attempt\":1,\"ordinal\":1,\"received_at\":\"2026-08-23T04:30:00Z\"}\n\n"
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" ||
		string(body) != want {
		t.Fatalf("post-header response = %d %q", response.StatusCode, body)
	}
	if !strings.Contains(logs.String(), "kind=corruption") ||
		strings.Contains(logs.String(), "secret Agent diagnostic") {
		t.Fatalf("safe stream log = %q", logs.String())
	}
}

type fakeTaskEventStreamOpener struct {
	runner taskEventRunner
	err    error
	taskID string
	after  uint64
}

func (opener *fakeTaskEventStreamOpener) OpenTaskEventStream(
	_ context.Context,
	taskID string,
	after uint64,
) (taskEventRunner, error) {
	opener.taskID = taskID
	opener.after = after
	return opener.runner, opener.err
}

type fakeTaskEventRunner struct {
	events []etcd.TaskEventRecord
	err    error
}

func (runner fakeTaskEventRunner) Run(
	ctx context.Context,
	emit func(etcd.TaskEventRecord) error,
) error {
	for _, event := range runner.events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(event); err != nil {
			return err
		}
	}
	return runner.err
}

func taskEventRouteTestServer(
	t *testing.T,
	opener taskEventStreamOpener,
	logOutput io.Writer,
) *httptest.Server {
	t.Helper()
	server := New(nil, slog.New(slog.NewTextHandler(logOutput, nil)), Options{})
	server.taskEventStreams = opener
	lifecycle := newHTTPLifecycle(server, productionHTTPPolicy())
	httpServer := httptest.NewServer(lifecycle.handler(server.HTTPHandler()))
	t.Cleanup(httpServer.Close)
	return httpServer
}

func requestTaskEventStream(t *testing.T, baseURL string, lastEventID string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodGet,
		baseURL+"/api/v1/tasks/"+taskEventRouteTestTaskID+"/events",
		nil,
	)
	if err != nil {
		t.Fatalf("build Task event request: %v", err)
	}
	request.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		request.Header.Set("Last-Event-ID", lastEventID)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("request Task events: %v", err)
	}
	return response
}
