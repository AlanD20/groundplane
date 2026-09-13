package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// QA: TASK-01; HTTP acceptance wake hints only, not durable Task publication or executor progress.
// Rationale: a 202 mutation must notify both executor owners exactly once so
// newly accepted work does not depend solely on periodic polling.
func TestAcceptedMutationWakesControllerAndAgentTaskRunners(t *testing.T) {
	controllerWakes := 0
	agentWakes := 0
	server := &Server{
		Mux:                http.NewServeMux(),
		controllerTaskWake: func() { controllerWakes++ },
		agentTaskWake:      func() { agentWakes++ },
		routePolicies:      make(map[string]routePolicy),
	}
	server.Mux.HandleFunc("POST /api/v1/actions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/actions", nil)
	request.Header.Set(idempotencyKeyHeader, "task-wake-test-0001")
	response := httptest.NewRecorder()
	server.serveAPIRequest(response, request)
	if response.Code != http.StatusAccepted || controllerWakes != 1 || agentWakes != 1 {
		t.Fatalf(
			"accepted response/wakes = %d/%d/%d, want %d/1/1",
			response.Code, controllerWakes, agentWakes, http.StatusAccepted,
		)
	}
}

// QA: TASK-01; one synchronous 204 response only, not all statuses or scheduling behavior.
// Rationale: a synchronous no-content mutation must not spuriously wake Task executors.
func TestNonAcceptedMutationDoesNotWakeTaskRunners(t *testing.T) {
	controllerWakes := 0
	agentWakes := 0
	server := &Server{
		Mux:                http.NewServeMux(),
		controllerTaskWake: func() { controllerWakes++ },
		agentTaskWake:      func() { agentWakes++ },
		routePolicies:      make(map[string]routePolicy),
	}
	server.Mux.HandleFunc("POST /api/v1/actions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/actions", nil)
	request.Header.Set(idempotencyKeyHeader, "task-wake-test-0002")
	response := httptest.NewRecorder()
	server.serveAPIRequest(response, request)
	if response.Code != http.StatusNoContent || controllerWakes != 0 || agentWakes != 0 {
		t.Fatalf(
			"non-accepted response/wakes = %d/%d/%d, want %d/0/0",
			response.Code, controllerWakes, agentWakes, http.StatusNoContent,
		)
	}
}
