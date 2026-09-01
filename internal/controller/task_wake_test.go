package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAcceptedMutationWakesControllerTaskRunner(t *testing.T) {
	wakes := 0
	server := &Server{
		Mux:                http.NewServeMux(),
		controllerTaskWake: func() { wakes++ },
		routePolicies:      make(map[string]routePolicy),
	}
	server.Mux.HandleFunc("POST /api/v1/actions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/actions", nil)
	request.Header.Set(idempotencyKeyHeader, "task-wake-test-0001")
	response := httptest.NewRecorder()
	server.serveAPIRequest(response, request)
	if response.Code != http.StatusAccepted || wakes != 1 {
		t.Fatalf("accepted response/wakes = %d/%d, want %d/1", response.Code, wakes, http.StatusAccepted)
	}
}

func TestNonAcceptedMutationDoesNotWakeControllerTaskRunner(t *testing.T) {
	wakes := 0
	server := &Server{
		Mux:                http.NewServeMux(),
		controllerTaskWake: func() { wakes++ },
		routePolicies:      make(map[string]routePolicy),
	}
	server.Mux.HandleFunc("POST /api/v1/actions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/actions", nil)
	request.Header.Set(idempotencyKeyHeader, "task-wake-test-0002")
	response := httptest.NewRecorder()
	server.serveAPIRequest(response, request)
	if response.Code != http.StatusNoContent || wakes != 0 {
		t.Fatalf("non-accepted response/wakes = %d/%d, want %d/0", response.Code, wakes, http.StatusNoContent)
	}
}
