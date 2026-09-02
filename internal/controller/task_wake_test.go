package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Rationale: a public mutation that durably accepts Task work must immediately
// hint both local and Agent executors without changing the response contract.
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

// Rationale: executor wakeups are reserved for accepted Task publications and
// must not run for synchronous mutations without queued work.
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
