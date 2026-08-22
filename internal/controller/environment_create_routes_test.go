package controller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeEnvironmentMutator struct {
	input          hierarchy.CreateEnvironmentInput
	idempotencyKey string
	response       etcd.IdempotencyResponse
	calls          int
}

func (mutator *fakeEnvironmentMutator) CreateEnvironment(
	_ context.Context,
	input hierarchy.CreateEnvironmentInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	mutator.calls++
	mutator.input = input
	mutator.idempotencyKey = idempotencyKey
	return mutator.response, nil
}

func TestEnvironmentCreateReturnsExactTaskAcceptance(t *testing.T) {
	mutator := &fakeEnvironmentMutator{response: etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		EnvironmentMutations: mutator,
	})
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/environments",
		io.NopCloser(stringReader(`{"project_id":"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"production"}`)),
	)
	request.Header.Set(idempotencyKeyHeader, "environment-create-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || response.Header().Get("Content-Type") != "application/json" ||
		response.Body.String() != `{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}` {
		t.Fatalf("response = %d %q %q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
	if mutator.calls != 1 || mutator.input.ProjectID != "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		mutator.input.Name != "production" || mutator.idempotencyKey != "environment-create-key-0001" {
		t.Fatalf("mutator call = %#v / %q", mutator.input, mutator.idempotencyKey)
	}
}

func TestEnvironmentCreateRejectsDuplicateOrUnknownMembers(t *testing.T) {
	mutator := &fakeEnvironmentMutator{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		EnvironmentMutations: mutator,
	})
	for name, body := range map[string]string{
		"duplicate": `{"project_id":"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"one","name":"two"}`,
		"unknown":   `{"project_id":"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"one","slug":"one"}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/environments", stringReader(body))
			response := httptest.NewRecorder()
			server.Mux.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
	if mutator.calls != 0 {
		t.Fatalf("mutator calls = %d, want 0", mutator.calls)
	}
}

type stringReader string

func (reader stringReader) Read(value []byte) (int, error) {
	if len(reader) == 0 {
		return 0, io.EOF
	}
	return copy(value, reader), io.EOF
}
