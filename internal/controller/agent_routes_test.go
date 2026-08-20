package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAgentRoutesMatchAcceptedHumanSurface(t *testing.T) {
	t.Parallel()

	server := &Server{Mux: http.NewServeMux()}
	server.routes()

	tests := []struct {
		name    string
		method  string
		path    string
		pattern string
	}{
		{name: "create task", method: http.MethodPost, path: "/api/v1/agents", pattern: "POST /api/v1/agents"},
		{name: "list", method: http.MethodGet, path: "/api/v1/agents", pattern: "GET /api/v1/agents"},
		{name: "show", method: http.MethodGet, path: "/api/v1/agents/agent_1", pattern: "GET /api/v1/agents/{id}"},
		{name: "remove task", method: http.MethodDelete, path: "/api/v1/agents/agent_1", pattern: "DELETE /api/v1/agents/{id}"},
		{name: "show config", method: http.MethodGet, path: "/api/v1/agents/agent_1/config", pattern: "GET /api/v1/agents/{id}/config"},
		{name: "set config", method: http.MethodPut, path: "/api/v1/agents/agent_1/config", pattern: "PUT /api/v1/agents/{id}/config"},
		{name: "update task", method: http.MethodPost, path: "/api/v1/agents/agent_1/update", pattern: "POST /api/v1/agents/{id}/update"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			_, pattern := server.Mux.Handler(request)
			if pattern != test.pattern {
				t.Fatalf("matched pattern = %q, want %q", pattern, test.pattern)
			}
		})
	}
}

func TestAgentJoinTokenRouteIsAbsent(t *testing.T) {
	t.Parallel()

	server := &Server{Mux: http.NewServeMux()}
	server.routes()

	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent-join-tokens", nil)
	_, pattern := server.Mux.Handler(request)
	if pattern != "" {
		t.Fatalf("retired route matched pattern %q", pattern)
	}

	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}
