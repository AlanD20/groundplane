package controller

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: only exact read-only POST operations may bypass mutation-key
// enforcement, never adjacent paths or other mutation methods.
// QA: UI-04, BP-01, BAK-14; local exact-path exemption rule.
func TestReadOnlyEnvironmentPostIdempotencyBoundary(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodPost, "/api/v1/environments/env_1/blueprint/validate", false},
		{http.MethodPost, "/api/v1/environments/env_1/export-key", false},
		{http.MethodPut, "/api/v1/environments/env_1/blueprint/validate", true},
		{http.MethodPost, "/api/v1/environments/env_1/blueprint/validate/extra", true},
		{http.MethodPost, "/api/v1/environments//blueprint/validate", true},
		{http.MethodPost, "/api/v1/projects/env_1/blueprint/validate", true},
		{http.MethodPut, "/api/v1/environments/env_1/blueprint", true},
	} {
		if got := requiresIdempotencyKey(httptest.NewRequest(test.method, test.path, nil)); got != test.want {
			t.Errorf("%s %s: requires key = %t, want %t", test.method, test.path, got, test.want)
		}
	}
}

// QA: UI-03/04; HTTP header admission only, not durable replay.
// Rationale: Invalid or repeated intent keys must fail before downstream mutation dispatch.
func TestMutationIdempotencyKeyBoundaryRejectsMissingAndInvalidHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "repeated", values: []string{"0123456789abcdef", "fedcba9876543210"}},
		{name: "too short", values: []string{"0123456789abcde"}},
		{name: "too long", values: []string{strings.Repeat("a", 129)}},
		{name: "invalid punctuation", values: []string{"0123456789abcde/"}},
		{name: "non ASCII", values: []string{"0123456789abcdeé"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server, calls := idempotencyBoundaryServer()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/resources", nil)
			for _, value := range test.values {
				request.Header.Add(idempotencyKeyHeader, value)
			}
			response := httptest.NewRecorder()

			server.requestHandler().ServeHTTP(response, request)

			if response.Code != http.StatusUnprocessableEntity {
				t.Fatalf(
					"status = %d, want %d; body=%s",
					response.Code,
					http.StatusUnprocessableEntity,
					response.Body.String(),
				)
			}
			if *calls != 0 {
				t.Fatalf("downstream calls = %d, want 0", *calls)
			}
			if contentType := response.Header().Get("Content-Type"); contentType != "application/problem+json" {
				t.Errorf("Content-Type = %q, want application/problem+json", contentType)
			}
			var problem errs.Problem
			if err := json.NewDecoder(response.Body).Decode(&problem); err != nil {
				t.Fatalf("decode problem: %v", err)
			}
			if problem.Code != errs.CodeValidationFailed {
				t.Errorf("problem code = %q, want %q", problem.Code, errs.CodeValidationFailed)
			}
			if problem.Type != errs.ProblemType {
				t.Errorf("problem type = %q, want %q", problem.Type, errs.ProblemType)
			}
		})
	}
}

// QA: UI-04; HTTP admission for mutation verbs only, not durable replay.
// Rationale: A valid explicit intent key must permit exactly one downstream call for each mutation verb.
func TestMutationIdempotencyKeyBoundaryAcceptsValidHeader(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			server, calls := idempotencyBoundaryServer()
			request := httptest.NewRequest(method, "/api/v1/resources", nil)
			request.Header.Set(idempotencyKeyHeader, "Abcdefghijk._:-0")
			response := httptest.NewRecorder()

			server.requestHandler().ServeHTTP(response, request)

			if response.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
			}
			if *calls != 1 {
				t.Fatalf("downstream calls = %d, want 1", *calls)
			}
		})
	}
}

// QA: UI-01/04; HTTP header policy only, not endpoint effects.
// Rationale: Read-only methods must not require a mutation intent key.
func TestIdempotencyKeyBoundaryDoesNotRequireHeaderForSafeMethods(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			server, calls := idempotencyBoundaryServer()
			request := httptest.NewRequest(method, "/api/v1/resources", nil)
			response := httptest.NewRecorder()

			server.requestHandler().ServeHTTP(response, request)

			if response.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
			}
			if *calls != 1 {
				t.Fatalf("downstream calls = %d, want 1", *calls)
			}
		})
	}
}

// Delivery: API-only middleware boundary; the fixture does not serve real operational routes.
// Rationale: Human mutation-key policy must not leak onto unrelated operational paths.
func TestIdempotencyKeyBoundaryDoesNotApplyOutsideAPIV1(t *testing.T) {
	t.Parallel()

	server, calls := idempotencyBoundaryServer()
	request := httptest.NewRequest(http.MethodPost, "/openapi.json", nil)
	response := httptest.NewRecorder()

	server.requestHandler().ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if *calls != 1 {
		t.Fatalf("downstream calls = %d, want 1", *calls)
	}
}

func idempotencyBoundaryServer() (*Server, *int) {
	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	})
	return &Server{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Mux:    mux,
	}, &calls
}
