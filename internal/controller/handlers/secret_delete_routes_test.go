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

// Rationale: Secret removal must expose the typed 202 operation while
// preserving the exact protected response and stable-id/idempotency inputs.
func TestSecretRemoveRouteReturnsExactTaskResponse(t *testing.T) {
	t.Parallel()
	want := testidempotency.IdempotencyResponse{
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
	request := httptest.NewRequest(
		http.MethodDelete,
		"/api/v1/secrets/"+secretID,
		bytes.NewBufferString(`{"after":["task_01ARZ3NDEKTSV4RRFFQ69G5FAW"]}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "secret-delete-key-0003")
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) || deleter.secretID != secretID ||
		deleter.key != "secret-delete-key-0003" || len(deleter.after) != 1 || deleter.after[0] != "task_01ARZ3NDEKTSV4RRFFQ69G5FAW" {
		t.Fatalf(
			"DELETE /secrets/{id} = %d/%q/%s, deleter = %#v",
			response.Code,
			response.Header().Get("Content-Type"),
			response.Body.Bytes(),
			deleter,
		)
	}
}

// Rationale: malformed typed prerequisite intent must be rejected before any
// protected mutation is dispatched.
func TestSecretRemoveRouteRejectsMalformedIntentBeforeDispatch(t *testing.T) {
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
		bytes.NewBufferString(`{"after":`),
	)
	request.Header.Set(idempotencyKeyHeader, "secret-delete-key-0004")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || deleter.calls != 0 {
		t.Fatalf("bodyful DELETE status/calls = %d/%d", response.Code, deleter.calls)
	}
}

type fakeSecretDeleter struct {
	response testidempotency.IdempotencyResponse
	secretID string
	key      string
	after    []string
	calls    int
}

func (deleter *fakeSecretDeleter) DeleteSecret(
	_ context.Context,
	secretID string,
	input apiTypes.TaskRunAfterRequest,
	key string,
) (testidempotency.IdempotencyResponse, error) {
	deleter.after = append([]string(nil), input.After...)
	deleter.calls++
	deleter.secretID = secretID
	deleter.key = key
	return deleter.response, nil
}
