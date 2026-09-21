package handlers

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

type fakeTaskRetrier struct {
	calls          int
	taskID         string
	idempotencyKey string
}

type fakeTaskAborter struct {
	calls          int
	taskID         string
	idempotencyKey string
}

func (aborter *fakeTaskAborter) AbortTask(
	_ context.Context,
	taskID string,
	idempotencyKey string,
) (testidempotencyowner.IdempotencyResponse, error) {
	aborter.calls++
	aborter.taskID = taskID
	aborter.idempotencyKey = idempotencyKey
	return testidempotencyowner.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"` + taskID + `"}`),
	}, nil
}

func (retrier *fakeTaskRetrier) RetryTask(
	_ context.Context,
	taskID string,
	idempotencyKey string,
) (testidempotencyowner.IdempotencyResponse, error) {
	retrier.calls++
	retrier.taskID = taskID
	retrier.idempotencyKey = idempotencyKey
	return testidempotencyowner.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_retry"}`),
	}, nil
}

// QA: TASK-02/05; HTTP dispatch only, not durable retry admission, replay or execution.
// Rationale: Retry must call its owner once with the exact Task id and replay key,
// then forward the accepted attempt identity without manufacturing another response.
func TestTaskRetryRouteUsesDurableRetrier(t *testing.T) {
	const taskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	retrier := &fakeTaskRetrier{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{TaskMutations: retrier})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+taskID+"/retry", nil)
	request.Header.Set(idempotencyKeyHeader, "task-retry-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || response.Header().Get("Content-Type") != "application/json" ||
		retrier.calls != 1 || retrier.taskID != taskID ||
		retrier.idempotencyKey != "task-retry-key-0001" || response.Body.String() != `{"task_id":"task_retry"}` {
		t.Fatalf("Task retry response/call = %d %s / %#v", response.Code, response.Body.String(), retrier)
	}
}

// QA: TASK-03; HTTP dispatch only, not cancellation, terminal publication or Abort races.
// Rationale: Abort must forward the addressed Task and replay key once, returning
// that same id rather than manufacturing a second cancellation Task.
func TestTaskAbortRouteReturnsTheTargetTaskIdentity(t *testing.T) {
	const taskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	aborter := &fakeTaskAborter{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{TaskAborts: aborter})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+taskID+"/abort", nil)
	request.Header.Set(idempotencyKeyHeader, "task-abort-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || response.Header().Get("Content-Type") != "application/json" ||
		aborter.calls != 1 || aborter.taskID != taskID ||
		aborter.idempotencyKey != "task-abort-key-0001" ||
		response.Body.String() != `{"task_id":"`+taskID+`"}` {
		t.Fatalf("Task abort response/call = %d %s / %#v", response.Code, response.Body.String(), aborter)
	}
}
