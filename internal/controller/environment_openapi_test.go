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
	type operation struct {
		OperationID string `json:"operationId"`
		RequestBody struct {
			Required bool                       `json:"required"`
			Content  map[string]json.RawMessage `json:"content"`
		} `json:"requestBody"`
		Responses map[string]struct {
			Content map[string]json.RawMessage `json:"content"`
		} `json:"responses"`
	}
	var contract struct {
		Paths map[string]map[string]operation `json:"paths"`
	}
	if err := json.Unmarshal(document, &contract); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	want := map[string]map[string]string{
		"/environments":                {"get": "environment.list", "post": "environment.create"},
		"/environments/{id}":           {"get": "environment.show", "patch": "environment.edit"},
		"/environments/{id}/blueprint": {"put": "environment.apply"},
		"/environments/{id}/rename":    {"post": "environment.rename"},
	}
	for path, methods := range want {
		for method, operationID := range methods {
			if got := contract.Paths[path][method].OperationID; got != operationID {
				t.Errorf("%s %s operationId = %q, want %q", method, path, got, operationID)
			}
		}
	}
	apply := contract.Paths["/environments/{id}/blueprint"]["put"]
	_, hasMultipart := apply.RequestBody.Content["multipart/form-data"]
	_, hasAcceptedJSON := apply.Responses["202"].Content["application/json"]
	if !apply.RequestBody.Required || len(apply.RequestBody.Content) != 1 || !hasMultipart || !hasAcceptedJSON {
		t.Fatalf("environment.apply request/response contract = %#v", apply)
	}
	edit := contract.Paths["/environments/{id}"]["patch"]
	_, hasEditJSON := edit.RequestBody.Content["application/json"]
	if !edit.RequestBody.Required || len(edit.RequestBody.Content) != 1 || !hasEditJSON {
		t.Fatalf("environment.edit request contract = %#v", edit.RequestBody)
	}
}
