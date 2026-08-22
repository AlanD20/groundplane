package controller

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Rationale: the scaffold must expose only the approved human routes; stale
// whole-entity replacements and detail routes become accidental API contracts.
func TestScaffoldRoutesMatchNormalizedHumanContract(t *testing.T) {
	t.Parallel()

	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})

	present := []struct {
		method  string
		path    string
		pattern string
	}{
		{http.MethodPost, "/api/v1/tenants/tnt_1/rename", "POST /api/v1/tenants/{id}/rename"},
		{http.MethodPost, "/api/v1/projects/prj_1/rename", "POST /api/v1/projects/{id}/rename"},
		{
			http.MethodPost,
			"/api/v1/environments/env_1/rename",
			"POST /api/v1/environments/{id}/rename",
		},
		{
			http.MethodPut,
			"/api/v1/environments/env_1/blueprint",
			"PUT /api/v1/environments/{id}/blueprint",
		},
		{http.MethodPatch, "/api/v1/services/svc_1", "PATCH /api/v1/services/{id}"},
		{http.MethodPost, "/api/v1/attaches", "POST /api/v1/attaches"},
		{http.MethodDelete, "/api/v1/attaches/att_1", "DELETE /api/v1/attaches/{id}"},
		{http.MethodGet, "/api/v1/entries/ev_1/value", "GET /api/v1/entries/{id}/value"},
		{http.MethodGet, "/api/v1/components/cmp_1", "GET /api/v1/components/{id}"},
		{http.MethodPost, "/api/v1/components/cmp_1/update", "POST /api/v1/components/{id}/update"},
		{http.MethodGet, "/api/v1/tasks/task_1/events", "GET /api/v1/tasks/{id}/events"},
		{http.MethodGet, "/api/v1/agents/agt_1/config", "GET /api/v1/agents/{id}/config"},
		{http.MethodGet, "/api/v1/runners/run_1", "GET /api/v1/runners/{id}"},
		{http.MethodDelete, "/api/v1/runners/run_1", "DELETE /api/v1/runners/{id}"},
	}
	for _, operation := range present {
		request := httptest.NewRequest(operation.method, operation.path, nil)
		_, pattern := server.Mux.Handler(request)
		if pattern != operation.pattern {
			t.Errorf(
				"%s %s matched %q, want %q",
				operation.method,
				operation.path,
				pattern,
				operation.pattern,
			)
		}
	}

	absent := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/api/v1/tenants/tnt_1"},
		{http.MethodPut, "/api/v1/projects/prj_1"},
		{http.MethodPut, "/api/v1/environments/env_1"},
		{http.MethodPut, "/api/v1/services/svc_1"},
		{http.MethodPut, "/api/v1/release-groups/rg_1"},
		{http.MethodPut, "/api/v1/zones/zon_1"},
		{http.MethodPut, "/api/v1/routes/rte_1"},
		{http.MethodPut, "/api/v1/volumes/vol_1"},
		{http.MethodPut, "/api/v1/entries/ent_1"},
		{http.MethodPut, "/api/v1/scripts/scr_1"},
		{http.MethodPost, "/api/v1/components"},
		{http.MethodPut, "/api/v1/components/cmp_1"},
		{http.MethodDelete, "/api/v1/components/cmp_1"},
		{http.MethodPut, "/api/v1/backing-services/prj_1"},
		{http.MethodGet, "/api/v1/attaches/att_1"},
	}
	for _, operation := range absent {
		request := httptest.NewRequest(operation.method, operation.path, nil)
		_, pattern := server.Mux.Handler(request)
		if pattern != "" {
			t.Errorf("retired route %s %s matched %q", operation.method, operation.path, pattern)
		}
	}
}

// Rationale: OpenAPI and Console use one stable identity, so the first typed
// operation must not retain the former presentation-oriented kebab id.
func TestHostOpenAPIOperationUsesParityIdentity(t *testing.T) {
	t.Parallel()

	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	document, err := json.Marshal(server.API.OpenAPI())
	if err != nil {
		t.Fatalf("marshal OpenAPI: %v", err)
	}
	if !strings.Contains(string(document), `"operationId":"host.show"`) {
		t.Fatalf("OpenAPI does not contain host.show: %s", document)
	}
	if strings.Contains(string(document), `"operationId":"host-show"`) {
		t.Fatalf("OpenAPI retains presentation-oriented host-show: %s", document)
	}
}
