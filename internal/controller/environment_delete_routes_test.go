package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type recordingEnvironmentDeleter struct {
	environmentID  string
	idempotencyKey string
}

func (deleter *recordingEnvironmentDeleter) DeleteEnvironment(
	_ context.Context,
	environmentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	deleter.environmentID = environmentID
	deleter.idempotencyKey = idempotencyKey
	return etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_x"}`),
	}, nil
}

// Rationale: destructive Environment deletion must dispatch through its tombstone-owning mutation service rather
// than the generic placeholder Task handler.
func TestEnvironmentDeleteDispatchesBodylessIntent(t *testing.T) {
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/environments/env_x", nil)
	request.SetPathValue("id", "env_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	request.Header.Set(idempotencyKeyHeader, "delete-environment-once")
	deleter := &recordingEnvironmentDeleter{}
	server := &Server{environmentDeletions: deleter}
	recorder := httptest.NewRecorder()

	server.environmentDelete(recorder, request)

	if recorder.Code != http.StatusAccepted || deleter.environmentID != "env_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		deleter.idempotencyKey != "delete-environment-once" {
		t.Fatalf("Environment delete dispatch = status %d, deleter %#v", recorder.Code, deleter)
	}
}
