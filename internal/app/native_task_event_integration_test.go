package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	api "github.com/AlanD20/groundplane/pkg/api"
)

// TASK-60: the production listener and SSE route replay only observations after
// the supplied cursor, drain a finished Task and reject an ahead/expired cursor
// before committing stream headers.
func TestNativeTaskHTTPEventReplayAndRetention(t *testing.T) {
	f := newNativeTaskInspectionFixture(t, io.Discard)
	view := f.volume.start(t, f.volume.created.TaskID)
	stage := workexecution.Stage{
		Key:       "prepare_storage",
		Name:      "Prepare persistent storage",
		State:     workexecution.StageRunning,
		Expected:  "Verify the captured data directory",
		StartedAt: view.Attempt.Attempt.StartedAt,
	}
	at := time.Now().UTC()
	if err := f.diagnostics.Report(t.Context(), view.Work.Task.Task.ID, view.Work.Item.Item.ID, view.Attempt.Attempt.ID, view.Attempt.Attempt.Executor, 1, stage, at); err != nil {
		t.Fatal(err)
	}
	finished := time.Now().UTC()
	stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &finished, "Captured storage ownership verified"
	if err := f.diagnostics.Report(t.Context(), view.Work.Task.Task.ID, view.Work.Item.Item.ID, view.Attempt.Attempt.ID, view.Attempt.Attempt.Executor, 2, stage, finished); err != nil {
		t.Fatal(err)
	}
	f.finish(t, view)
	base := nativeTaskEventServer(t, f, io.Discard)
	response := nativeTaskEventRequest(t, base, view.Work.Task.Task.ID, "1")
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK ||
		response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("event stream: status=%d, %v", response.StatusCode, err)
	}
	frames := strings.Split(strings.TrimSpace(string(body)), "\n\n")
	for index, frame := range frames {
		lines := strings.Split(frame, "\n")
		expected := uint64(index + 2)
		if len(lines) != 2 || lines[0] != "id: "+strconv.FormatUint(expected, 10) ||
			!strings.HasPrefix(lines[1], "data: ") {
			t.Fatalf("resume did not return complete contiguous frames after cursor 1: %q", body)
		}
		var event api.TaskEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event.Sequence != expected || event.AttemptSequence != expected ||
			event.WorkItemID != view.Work.Item.Item.ID ||
			event.AttemptID != view.Attempt.Attempt.ID {
			t.Fatal("SSE lost native work/attempt identity or cursor ordering")
		}
		if index == 0 && (event.Stage.Observed != stage.Observed || event.Stage.Name != stage.Name) {
			t.Fatal("SSE lost useful observed evidence at the resumed boundary")
		}
	}
	ahead := nativeTaskEventRequest(t, base, view.Work.Task.Task.ID, strconv.Itoa(len(frames)+2))
	defer ahead.Body.Close()
	if ahead.StatusCode != http.StatusUnprocessableEntity ||
		!strings.HasPrefix(ahead.Header.Get("Content-Type"), "application/problem+json") {
		t.Fatalf("ahead cursor committed SSE headers: %d", ahead.StatusCode)
	}
	task, _, err := f.volume.queue.ReadTask(t.Context(), view.Work.Task.Task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	done, err := f.diagnostics.ExpireTask(
		t.Context(),
		view.Work.Task.Task.ID,
		task.Task.FinishedAt.Add(2*time.Hour),
		time.Hour,
	)
	if err != nil || !done {
		t.Fatalf("expire event tail: %v", err)
	}
	expired := nativeTaskEventRequest(t, base, view.Work.Task.Task.ID, "1")
	defer expired.Body.Close()
	if expired.StatusCode != http.StatusConflict ||
		!strings.HasPrefix(expired.Header.Get("Content-Type"), "application/problem+json") {
		t.Fatalf("expired cursor silently skipped history: %d", expired.StatusCode)
	}
	var problem struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(expired.Body).Decode(&problem); err != nil || problem.Code != "cursor.expired" {
		t.Fatalf("expired diagnostic interval did not identify the history gap: %q, %v", problem.Code, err)
	}
}

// TASK-60: loss of diagnostic storage after a delivered frame closes the stream
// without changing that frame or leaking the internal error in logs/output.
func TestNativeTaskHTTPEventFailureAfterHeaders(t *testing.T) {
	f := newNativeTaskInspectionFixture(t, io.Discard)
	view := f.volume.start(t, f.volume.created.TaskID)
	stage := workexecution.Stage{
		Key:       "prepare_storage",
		Name:      "Prepare persistent storage",
		State:     workexecution.StageRunning,
		Expected:  "Verify captured storage ownership",
		StartedAt: view.Attempt.Attempt.StartedAt,
	}
	if err := f.diagnostics.Report(t.Context(), view.Work.Task.Task.ID, view.Work.Item.Item.ID, view.Attempt.Attempt.ID,
		view.Attempt.Attempt.Executor, 1, stage, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var logs nativeTaskLogBuffer
	base := nativeTaskEventServer(t, f, &logs)
	response := nativeTaskEventRequest(t, base, view.Work.Task.Task.ID, "")
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	var frame strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		frame.WriteString(line)
		if line == "\n" {
			break
		}
	}
	if !strings.HasPrefix(frame.String(), "id: 1\ndata: ") {
		t.Fatalf("initial frame: %q", frame.String())
	}
	f.fault.fail.Store(true)
	tail, err := io.ReadAll(reader)
	if err != nil || len(tail) != 0 || response.StatusCode != http.StatusOK {
		t.Fatalf("stream failure corrupted delivered evidence: tail=%q, %v", tail, err)
	}
	if !strings.Contains(logs.String(), "kind=corruption") ||
		strings.Contains(logs.String(), "private diagnostic failure detail") ||
		strings.Contains(frame.String(), "private diagnostic failure detail") {
		t.Fatal("stream failure did not use a safe diagnostic classification")
	}
}

// Observe the real bound address from the existing structured startup log; do
// not reserve and release a port or add a test-only production listener API.
type nativeTaskListenerLog struct {
	base  slog.Handler
	bound chan string
}

func (handler nativeTaskListenerLog) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.base.Enabled(ctx, level)
}
func (handler nativeTaskListenerLog) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "controller: listening" {
		record.Attrs(func(value slog.Attr) bool {
			if value.Key == "addr" {
				handler.bound <- value.Value.String()
			}
			return true
		})
	}
	return handler.base.Handle(ctx, record)
}
func (handler nativeTaskListenerLog) WithAttrs(attrs []slog.Attr) slog.Handler {
	return nativeTaskListenerLog{base: handler.base.WithAttrs(attrs), bound: handler.bound}
}
func (handler nativeTaskListenerLog) WithGroup(name string) slog.Handler {
	return nativeTaskListenerLog{base: handler.base.WithGroup(name), bound: handler.bound}
}

func nativeTaskEventServer(t *testing.T, f *nativeTaskInspectionFixture, logs io.Writer) string {
	t.Helper()
	bound := make(chan string, 1)
	f.server.Logger = slog.New(nativeTaskListenerLog{base: slog.NewTextHandler(logs, nil), bound: bound})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- f.server.Serve(ctx, []string{"127.0.0.1:0"}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("native HTTP listener did not stop")
		}
	})
	select {
	case address := <-bound:
		return "http://" + address
	case err := <-done:
		done <- err
		t.Fatalf("native HTTP listener: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("native HTTP listener did not bind")
	}
	return ""
}

func nativeTaskEventRequest(t *testing.T, base, taskID, after string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/api/v1/tasks/"+taskID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept", "text/event-stream")
	if after != "" {
		request.Header.Set("Last-Event-ID", after)
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

type nativeTaskLogBuffer struct {
	mu    sync.Mutex
	bytes bytes.Buffer
}

func (buffer *nativeTaskLogBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.bytes.Write(value)
}
func (buffer *nativeTaskLogBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.bytes.String()
}
