package controller

import (
	"encoding/json"
	"testing"
)

// Rationale: a Project route is migrated only when the serving Huma object,
// and therefore both generated clients, exposes every synchronous action.
// Delivery: operation identity inventory, not Project effects.
func TestProjectOpenAPIContainsServingOperations(t *testing.T) {
	t.Parallel()
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	var contract struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(document, &contract); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	want := map[string]map[string]string{
		"/projects":             {"get": "project.list", "post": "project.create"},
		"/projects/{id}":        {"get": "project.show", "patch": "project.edit"},
		"/projects/{id}/rename": {"post": "project.rename"},
	}
	for path, methods := range want {
		for method, operationID := range methods {
			if got := contract.Paths[path][method].OperationID; got != operationID {
				t.Errorf("%s %s operationId = %q, want %q", method, path, got, operationID)
			}
		}
	}
}
