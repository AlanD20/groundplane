package controller

import (
	"encoding/json"
	"testing"
)

// Rationale: an Environment route is migrated only when the serving Huma
// object, and therefore both generated clients, exposes every synchronous action.
func TestEnvironmentOpenAPIContainsServingOperations(t *testing.T) {
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
		"/environments":             {"get": "environment.list", "post": "environment.create"},
		"/environments/{id}":        {"get": "environment.show"},
		"/environments/{id}/rename": {"post": "environment.rename"},
	}
	for path, methods := range want {
		for method, operationID := range methods {
			if got := contract.Paths[path][method].OperationID; got != operationID {
				t.Errorf("%s %s operationId = %q, want %q", method, path, got, operationID)
			}
		}
	}
}
