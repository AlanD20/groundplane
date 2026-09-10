package controller

import "testing"

// Rationale: Go object pointers do not automatically produce nullable OpenAPI
// references. Empty update metadata must remain null in both generated clients.
func TestControllerUpdateSnapshotSchemaPreservesAbsence(t *testing.T) {
	t.Parallel()
	server := New(nil, nil, Options{})
	schema := server.API.OpenAPI().Components.Schemas.SchemaFromRef("#/components/schemas/ControllerUpdateState")
	for _, field := range []string{"candidate", "last_update"} {
		variants := schema.Properties[field].OneOf
		if len(variants) != 2 || variants[0].Ref == "" || variants[1].Type != "null" {
			t.Fatalf("field %s has no nullable reference: %#v", field, variants)
		}
	}
}
