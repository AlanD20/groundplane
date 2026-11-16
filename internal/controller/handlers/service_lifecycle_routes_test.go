package handlers

import (
	"context"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

type fakeServiceLifecycleMutator struct {
	ServiceMutator
	action         string
	serviceID      string
	idempotencyKey string
	after          []string
}

func (fake *fakeServiceLifecycleMutator) StartService(
	_ context.Context,
	serviceID string,
	input apiTypes.TaskRunAfterRequest,
	key string,
) (testidempotencyowner.IdempotencyResponse, error) {
	fake.after = append([]string(nil), input.After...)
	return fake.record("start", serviceID, key), nil
}

func (fake *fakeServiceLifecycleMutator) StopService(
	_ context.Context,
	serviceID string,
	input apiTypes.TaskRunAfterRequest,
	key string,
) (testidempotencyowner.IdempotencyResponse, error) {
	fake.after = append([]string(nil), input.After...)
	return fake.record("stop", serviceID, key), nil
}

func (fake *fakeServiceLifecycleMutator) DestroyService(
	_ context.Context,
	serviceID string,
	input apiTypes.TaskRunAfterRequest,
	key string,
) (testidempotencyowner.IdempotencyResponse, error) {
	fake.after = append([]string(nil), input.After...)
	return fake.record("destroy", serviceID, key), nil
}

func (fake *fakeServiceLifecycleMutator) record(
	action string,
	serviceID string,
	key string,
) testidempotencyowner.IdempotencyResponse {
	fake.action = action
	fake.serviceID = serviceID
	fake.idempotencyKey = key
	return testidempotencyowner.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_action"}`),
	}
}

func TestServiceLifecycleRoutesDispatchProtectedActionsWithPrerequisites(t *testing.T) {
	// Rationale: the three operator actions must remain 1:1 typed API
	// operations and preserve the stable Service id plus replay key.
	t.Parallel()
	serviceID := ids.NewAt(ids.KindService, serviceRouteTestTime(), 80)
	for _, action := range []string{"start", "stop", "destroy"} {
		mutator := &fakeServiceLifecycleMutator{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ServiceMutations: mutator})
		request := httptest.NewRequest(
			http.MethodPost,
			"/api/v1/services/"+serviceID+"/"+action,
			strings.NewReader(`{"after":["task_01ARZ3NDEKTSV4RRFFQ69G5FAV"]}`),
		)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "service-action-key-0001")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted || response.Body.String() != `{"task_id":"task_action"}` ||
			mutator.action != action || mutator.serviceID != serviceID ||
			mutator.idempotencyKey != "service-action-key-0001" || len(mutator.after) != 1 || mutator.after[0] != "task_01ARZ3NDEKTSV4RRFFQ69G5FAV" {
			t.Fatalf("%s response/call = %d %s / %#v", action, response.Code, response.Body.String(), mutator)
		}
	}
}

func TestServiceLifecycleRoutesRejectBodyAndQuery(t *testing.T) {
	// Rationale: malformed lifecycle requests must be rejected;
	// accepting ignored input would create an undocumented second contract.
	t.Parallel()
	serviceID := ids.NewAt(ids.KindService, serviceRouteTestTime(), 81)
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		ServiceMutations: &fakeServiceLifecycleMutator{},
	})
	for name, request := range map[string]*http.Request{
		"body":  httptest.NewRequest(http.MethodPost, "/api/v1/services/"+serviceID+"/stop", io.NopCloser(io.LimitReader(&zeroReader{}, 1))),
		"query": httptest.NewRequest(http.MethodPost, "/api/v1/services/"+serviceID+"/stop?grace=1", nil),
	} {
		request.Header.Set(idempotencyKeyHeader, "service-action-key-0002")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s response = %d %s", name, response.Code, response.Body.String())
		}
	}
}

type zeroReader struct{}

func (*zeroReader) Read(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	value[0] = 0
	return 1, io.EOF
}
