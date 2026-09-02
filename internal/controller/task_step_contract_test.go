package controller

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/danielgtaylor/huma/v2"
)

func TestTaskStepKindResponseRejectsUnclosedScriptStep(t *testing.T) {
	// Rationale: a durable RunScript step with missing identity must fail closed
	// instead of being projected as an ordinary Task step.
	for name, step := range map[string]etcd.TaskStepRecord{
		"missing kind":       {ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		"missing identity":   {Kind: etcd.TaskStepScript, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		"operation identity": {Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ScriptID: "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV", ScriptSlug: "migrate"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := taskStepKindResponse(step); err == nil {
				t.Fatalf("taskStepKindResponse(%#v) succeeded", step)
			}
		})
	}
}

func TestTaskStepOpenAPISchemaRequiresClosedVariant(t *testing.T) {
	// Rationale: generated clients must see one paired Script identity contract,
	// not two independently optional fields that admit corrupt Task evidence.
	registry := huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer)
	reference := taskStepOpenAPISchema(registry)
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		t.Fatal("TaskStep schema is missing")
	}
	if got := schema.DependentRequired["script_id"]; !slices.Equal(got, []string{"script_slug"}) {
		t.Fatalf("script_id dependentRequired = %v", got)
	}
	if got := schema.DependentRequired["script_slug"]; !slices.Equal(got, []string{"script_id"}) {
		t.Fatalf("script_slug dependentRequired = %v", got)
	}
	if len(schema.OneOf) != 2 {
		t.Fatalf("TaskStep oneOf variants = %d, want 2", len(schema.OneOf))
	}
	variants := make(map[any]*huma.Schema, len(schema.OneOf))
	for _, variant := range schema.OneOf {
		variants[variant.Properties["kind"].Const] = variant
	}
	operation := variants["operation"]
	if operation == nil || operation.Not == nil {
		t.Fatal("operation TaskStep exclusion is missing")
	}
	script := variants["script"]
	if script == nil || !slices.Contains(script.Required, "script_id") || !slices.Contains(script.Required, "script_slug") {
		t.Fatalf("script TaskStep required fields = %v", script.Required)
	}
}
