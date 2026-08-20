package controller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestTaskRetryRouteReturnsNewAttempt(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	original, err := server.dispatcher.Dispatch(context.Background(), DispatchRequest{Type: TaskScript, Target: "svc_1", IdempotencyKey: "operation-key", PlanHash: "plan-hash"})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	original.Status = StatusTimedOut

	request := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+original.ID+"/retry", nil)
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusAccepted, response.Body.String())
	}
	var accepted apiTypes.TaskAccepted
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if accepted.TaskID == "" || accepted.TaskID == original.ID {
		t.Fatalf("task_id = %q, want new attempt", accepted.TaskID)
	}
}
