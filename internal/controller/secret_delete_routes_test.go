package controller

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: Secret removal must expose the typed 202 operation while
// preserving the exact protected response and stable-id/idempotency inputs.
func TestSecretRemoveRouteReturnsExactTaskResponse(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	deleter := &fakeSecretDeleter{response: want}
	server := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options{SecretDeletions: deleter},
	)
	secretID := "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/secrets/"+secretID, nil)
	request.Header.Set(idempotencyKeyHeader, "secret-delete-key-0003")
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) || deleter.secretID != secretID ||
		deleter.key != "secret-delete-key-0003" {
		t.Fatalf(
			"DELETE /secrets/{id} = %d/%q/%s, deleter = %#v",
			response.Code,
			response.Header().Get("Content-Type"),
			response.Body.Bytes(),
			deleter,
		)
	}
}

// Rationale: DELETE intent is bodyless and must reject bytes before any
// protected mutation is dispatched.
func TestSecretRemoveRouteRejectsBodiesBeforeDispatch(t *testing.T) {
	t.Parallel()
	deleter := &fakeSecretDeleter{}
	server := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options{SecretDeletions: deleter},
	)
	request := httptest.NewRequest(
		http.MethodDelete,
		"/api/v1/secrets/sec_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		bytes.NewBufferString(`{}`),
	)
	request.Header.Set(idempotencyKeyHeader, "secret-delete-key-0004")
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || deleter.calls != 0 {
		t.Fatalf("bodyful DELETE status/calls = %d/%d", response.Code, deleter.calls)
	}
}

type fakeSecretDeleter struct {
	response etcd.IdempotencyResponse
	secretID string
	key      string
	calls    int
}

func (deleter *fakeSecretDeleter) DeleteSecret(
	_ context.Context,
	secretID string,
	key string,
) (etcd.IdempotencyResponse, error) {
	deleter.calls++
	deleter.secretID = secretID
	deleter.key = key
	return deleter.response, nil
}
