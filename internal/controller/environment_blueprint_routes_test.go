package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type recordingEnvironmentBlueprintMutator struct {
	environmentID  string
	idempotencyKey string
	bundle         core.BlueprintBundle
}

func (mutator *recordingEnvironmentBlueprintMutator) ApplyBlueprint(
	_ context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	mutator.environmentID = environmentID
	mutator.idempotencyKey = idempotencyKey
	mutator.bundle = bundle
	return etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_x"}`),
	}, nil
}

// Rationale: the public Blueprint route must pass the verified logical bundle and Environment-scoped idempotency key
// to exactly one durable mutator rather than the generic placeholder Task handler.
func TestEnvironmentBlueprintApplyDispatchesVerifiedBundle(t *testing.T) {
	content := []byte("services: {}\n")
	request := blueprintMultipartTestRequest(t, blueprintTestManifest(content), []blueprintTestPart{{
		name: "file-000001", contentType: "application/octet-stream", content: content,
	}})
	request.SetPathValue("id", "env_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	request.Header.Set(idempotencyKeyHeader, "apply-once")
	mutator := &recordingEnvironmentBlueprintMutator{}
	server := &Server{environmentBlueprints: mutator}
	recorder := httptest.NewRecorder()

	server.environmentBlueprintApply(recorder, request)

	if recorder.Code != http.StatusAccepted || mutator.environmentID != "env_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		mutator.idempotencyKey != "apply-once" || mutator.bundle.RootPath != "blueprint.yaml" ||
		len(mutator.bundle.Files) != 1 {
		t.Fatalf("Blueprint dispatch = status %d, mutator %#v", recorder.Code, mutator)
	}
}
