package handlers

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the serving create route must preserve the exact protected
// response while decoding every write-only field without reflecting value.
func TestSecretCreateRoutePreservesExactMutationResponse(t *testing.T) {
	t.Parallel()
	want := testidempotency.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body: []byte(
			`{"id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV","scope":"platform","key":"TOKEN","kind":"env_var","ref":"secrets/.env.edge","updated_at":"2026-08-22T19:00:00Z"}`,
		),
	}
	mutator := &fakeSecretMutator{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{SecretMutations: mutator})
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/secrets",
		bytes.NewBufferString(`{"platform":true,"key":"TOKEN","kind":"env_var","value":"private"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "secret-create-key-0004")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) {
		t.Fatalf(
			"POST /secrets status/content/body = %d/%q/%q",
			response.Code,
			response.Header().Get("Content-Type"),
			response.Body.Bytes(),
		)
	}
	if !mutator.called || !mutator.input.Platform || mutator.input.Key != "TOKEN" ||
		mutator.input.Kind != "env_var" || mutator.input.Value != "private" ||
		mutator.key != "secret-create-key-0004" {
		t.Fatalf("CreateSecret() did not receive the exact decoded request")
	}
}

// Rationale: duplicate, unknown, wrong-typed, and trailing JSON must fail
// before a plaintext-bearing request reaches the application boundary.
func TestSecretCreateRouteRejectsNonCanonicalBodies(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"platform":true,"key":"A","key":"B","kind":"env_var","value":"x"}`,
		`{"platform":true,"key":"A","kind":"env_var","value":"x","extra":true}`,
		`{"platform":"true","key":"A","kind":"env_var","value":"x"}`,
		`{"platform":true,"key":"A","kind":"env_var","value":"x"}{}`,
	} {
		mutator := &fakeSecretMutator{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{SecretMutations: mutator})
		request := httptest.NewRequest(http.MethodPost, "/api/v1/secrets", bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "secret-create-key-0005")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code < 400 || response.Code >= 500 || mutator.called {
			t.Fatalf("non-canonical Secret body status/called = %d/%t", response.Code, mutator.called)
		}
	}
}

type fakeSecretMutator struct {
	input    apiTypes.SecretCreateRequest
	key      string
	response testidempotency.IdempotencyResponse
	called   bool
}

func (mutator *fakeSecretMutator) CreateSecret(
	_ context.Context,
	input apiTypes.SecretCreateRequest,
	key string,
) (testidempotency.IdempotencyResponse, error) {
	mutator.called = true
	mutator.input = input
	mutator.key = key
	return mutator.response, nil
}
