package controller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeTaskRetrier struct {
	taskID         string
	idempotencyKey string
}

func (retrier *fakeTaskRetrier) RetryTask(
	_ context.Context,
	taskID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	retrier.taskID = taskID
	retrier.idempotencyKey = idempotencyKey
	return etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_retry"}`),
	}, nil
}

// Rationale: the public retry route must dispatch the durable application service with the exact Task id and replay key, never the removed process-local Dispatcher.
func TestTaskRetryRouteUsesDurableRetrier(t *testing.T) {
	const taskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	retrier := &fakeTaskRetrier{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{TaskMutations: retrier})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+taskID+"/retry", nil)
	request.Header.Set(idempotencyKeyHeader, "task-retry-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || retrier.taskID != taskID ||
		retrier.idempotencyKey != "task-retry-key-0001" || response.Body.String() != `{"task_id":"task_retry"}` {
		t.Fatalf("Task retry response/call = %d %s / %#v", response.Code, response.Body.String(), retrier)
	}
}
