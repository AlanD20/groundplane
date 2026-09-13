package controller

import (
	"errors"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

// QA: TASK-01, SCRIPT-03; pure step projection only, not durable validation or hook execution.
// Rationale: incomplete Script identity or identity on an operation step must
// fail as corrupt state, never appear as a valid ordinary step.
func TestTaskStepKindResponseRejectsUnclosedScriptStep(t *testing.T) {
	for name, step := range map[string]etcd.TaskStepRecord{
		"missing kind":       {ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		"missing identity":   {Kind: etcd.TaskStepScript, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		"operation identity": {Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ScriptID: "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV", ScriptSlug: "migrate"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := taskStepKindResponse(step); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("taskStepKindResponse(%#v) = %v, want internal", step, err)
			}
		})
	}
}

// Delivery: generated step schema only; not HTTP response validation or actual Script execution.
// Rationale: Script fields must be paired and forbidden on operation steps,
// preventing generated schemas from admitting contradictory Task evidence.
func TestTaskStepOpenAPISchemaRequiresClosedVariant(t *testing.T) {
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
	if operation == nil || operation.Not == nil || len(operation.Not.AnyOf) != 2 {
		t.Fatal("operation TaskStep exclusion is missing")
	}
	if !slices.Equal(operation.Not.AnyOf[0].Required, []string{"script_id"}) ||
		!slices.Equal(operation.Not.AnyOf[1].Required, []string{"script_slug"}) {
		t.Fatalf("operation TaskStep exclusion = %#v", operation.Not)
	}
	script := variants["script"]
	if script == nil || !slices.Contains(script.Required, "script_id") ||
		!slices.Contains(script.Required, "script_slug") {
		t.Fatalf("script TaskStep schema = %#v", script)
	}
}
