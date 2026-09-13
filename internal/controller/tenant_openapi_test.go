package controller

import (
	"encoding/json"
	"testing"
)

// Rationale: a Tenant route is migrated only when the live Huma object, and
// therefore both generated clients, exposes every synchronous Tenant action.
// Delivery: operation identity inventory, not Tenant effects.
func TestTenantOpenAPIContainsServingOperations(t *testing.T) {
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
		"/tenants": {
			"get": "tenant.list", "post": "tenant.create",
		},
		"/tenants/{id}": {
			"get": "tenant.show", "patch": "tenant.edit",
		},
		"/tenants/{id}/rename": {
			"post": "tenant.rename",
		},
	}
	for path, methods := range want {
		for method, operationID := range methods {
			if got := contract.Paths[path][method].OperationID; got != operationID {
				t.Errorf("%s %s operationId = %q, want %q", method, path, got, operationID)
			}
		}
	}
}
