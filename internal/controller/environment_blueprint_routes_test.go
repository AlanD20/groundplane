package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type recordingEnvironmentBlueprintMutator struct {
	environmentID    string
	expectedRevision string
	idempotencyKey   string
	bundle           core.BlueprintBundle
}

// Rationale: validation is read-only and must work through the production
// middleware without a mutation idempotency key; apply must still require one.
func TestBlueprintValidationDoesNotRequireMutationKey(t *testing.T) {
	t.Parallel()
	for _, validation := range []bool{true, false} {
		content := []byte("services: {}\n")
		request := blueprintMultipartTestRequest(t, blueprintTestManifest(content), []blueprintTestPart{{
			name: "file-000001", contentType: "application/octet-stream", content: content,
		}})
		request.URL.Path = "/api/v1/environments/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/blueprint"
		request.Method = http.MethodPut
		want := http.StatusUnprocessableEntity
		if validation {
			request.Method = http.MethodPost
			request.URL.Path += "/validate"
			want = http.StatusOK
		}
		request.Header.Set("If-Match", "\"0\"")
		request.Header.Del(idempotencyKeyHeader)
		server := New(nil, nil, Options{EnvironmentBlueprints: &recordingEnvironmentBlueprintMutator{}})
		response := httptest.NewRecorder()
		server.requestHandler().ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("validation=%t: status=%d, want %d: %s", validation, response.Code, want, response.Body.String())
		}
	}
}

func (mutator *recordingEnvironmentBlueprintMutator) GetBlueprint(
	_ context.Context,
	environmentID string,
) (apiTypes.EnvironmentBlueprintDocument, error) {
	return apiTypes.EnvironmentBlueprintDocument{
		EnvironmentID: environmentID,
		Revision:      apiTypes.EnvironmentBlueprintInitialRevision,
		Document:      "services: {}\n",
	}, nil
}

func (mutator *recordingEnvironmentBlueprintMutator) ValidateBlueprint(
	_ context.Context,
	_ string,
	_ core.BlueprintBundle,
	expectedRevision string,
) (apiTypes.EnvironmentBlueprintValidation, error) {
	return apiTypes.EnvironmentBlueprintValidation{
		Revision: expectedRevision,
		Changes:  []apiTypes.EnvironmentBlueprintChange{},
	}, nil
}

func (mutator *recordingEnvironmentBlueprintMutator) ApplyBlueprint(
	_ context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	mutator.environmentID = environmentID
	mutator.expectedRevision = expectedRevision
	mutator.idempotencyKey = idempotencyKey
	mutator.bundle = bundle
	return etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_x"}`),
	}, nil
}

// Rationale: the public Blueprint route must pass the verified logical bundle and Environment-scoped idempotency key
// to exactly one durable mutator rather than the generic placeholder Task handler.
func TestEnvironmentBlueprintApplyDispatchesVerifiedBundle(t *testing.T) {
	t.Parallel()
	content := []byte("services: {}\n")
	request := blueprintMultipartTestRequest(t, blueprintTestManifest(content), []blueprintTestPart{{
		name: "file-000001", contentType: "application/octet-stream", content: content,
	}})
	request.URL.Path = "/api/v1/environments/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/blueprint"
	request.Header.Set("If-Match", "\"0\"")
	request.Header.Set(idempotencyKeyHeader, "environment-apply-key-0001")
	mutator := &recordingEnvironmentBlueprintMutator{}
	server := New(nil, nil, Options{EnvironmentBlueprints: mutator})
	recorder := httptest.NewRecorder()

	server.Mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted || mutator.environmentID != "env_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		mutator.expectedRevision != apiTypes.EnvironmentBlueprintInitialRevision ||
		mutator.idempotencyKey != "environment-apply-key-0001" || mutator.bundle.RootPath != "blueprint.yaml" ||
		len(mutator.bundle.Files) != 1 {
		t.Fatalf("Blueprint dispatch = status %d, mutator %#v", recorder.Code, mutator)
	}
}
