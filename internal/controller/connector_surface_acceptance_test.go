package controller

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Rationale: API parity is closed to list/add/show/remove; a CRUD check,
// rename, edit, or alternate ownership route would contradict ADR 0045.
func TestC15ConnectorAPIHasExactPublicOperations(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	for _, operation := range []struct {
		method  string
		path    string
		pattern string
	}{
		{http.MethodGet, "/api/v1/connectors?environment=env_1", "GET /api/v1/connectors"},
		{http.MethodPost, "/api/v1/connectors?environment=env_1", "POST /api/v1/connectors"},
		{http.MethodGet, "/api/v1/connectors/con_1", "GET /api/v1/connectors/{id}"},
		{http.MethodDelete, "/api/v1/connectors/con_1", "DELETE /api/v1/connectors/{id}"},
	} {
		_, pattern := server.Mux.Handler(httptest.NewRequest(operation.method, operation.path, nil))
		if pattern != operation.pattern {
			t.Errorf("%s %s pattern = %q, want %q", operation.method, operation.path, pattern, operation.pattern)
		}
	}
	for _, operation := range []struct{ method, path string }{
		{http.MethodPatch, "/api/v1/connectors/con_1"},
		{http.MethodPut, "/api/v1/connectors/con_1"},
		{http.MethodPost, "/api/v1/connectors/con_1/rename"},
		{http.MethodPost, "/api/v1/connectors/con_1/check"},
	} {
		_, pattern := server.Mux.Handler(httptest.NewRequest(operation.method, operation.path, nil))
		if pattern != "" {
			t.Errorf("retired Connector route %s %s matched %q", operation.method, operation.path, pattern)
		}
	}
}
