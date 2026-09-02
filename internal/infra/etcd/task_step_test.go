package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: retry and repository copies must preserve immutable Script evidence
// on its exact Task step while ordinary steps remain unannotated.
func TestTaskStepScriptIdentityValidatesAndClones(t *testing.T) {
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	steps := []TaskStepRecord{
		{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, at, 1)},
		{Kind: TaskStepScript, ID: ids.NewAt(ids.KindStep, at, 2),
			ScriptID:   ids.NewAt(ids.KindScript, at, 3),
			ScriptSlug: "migrate-schema",
		},
	}
	if err := validateTaskSteps(steps); err != nil {
		t.Fatalf("validateTaskSteps() error = %v", err)
	}
	cloned := cloneTaskSteps(steps)
	steps[1].ScriptID = ""
	steps[1].ScriptSlug = ""
	if cloned[0].ScriptID != "" || cloned[0].ScriptSlug != "" ||
		cloned[1].ScriptID == "" || cloned[1].ScriptSlug != "migrate-schema" {
		t.Fatalf("cloneTaskSteps() = %#v", cloned)
	}
}

// Rationale: a partial or malformed Script identity cannot be interpreted
// unambiguously by public Task readers and must fail at the durable boundary.
func TestTaskStepScriptIdentityRejectsPartialOrInvalidMetadata(t *testing.T) {
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	stepID := ids.NewAt(ids.KindStep, at, 10)
	scriptID := ids.NewAt(ids.KindScript, at, 11)
	tests := map[string]TaskStepRecord{
		"missing Script slug": {ID: stepID, ScriptID: scriptID},
		"missing Script id":   {ID: stepID, ScriptSlug: "migrate-schema"},
		"invalid Script id":   {ID: stepID, ScriptID: "scr_invalid", ScriptSlug: "migrate-schema"},
		"invalid Script slug": {ID: stepID, ScriptID: scriptID, ScriptSlug: "Migrate Schema"},
	}
	for name, step := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateTaskSteps([]TaskStepRecord{step}); err == nil {
				t.Fatalf("validateTaskSteps(%#v) succeeded", step)
			}
		})
	}
}

// Rationale: duplicate Script ids or slugs would make one public identity point
// at multiple durable steps, so the Task record must reject either ambiguity.
func TestTaskStepScriptIdentityRejectsAmbiguousDuplicates(t *testing.T) {
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	stepA := ids.NewAt(ids.KindStep, at, 20)
	stepB := ids.NewAt(ids.KindStep, at, 21)
	scriptA := ids.NewAt(ids.KindScript, at, 22)
	scriptB := ids.NewAt(ids.KindScript, at, 23)
	tests := map[string][]TaskStepRecord{
		"duplicate Script id": {
			{ID: stepA, ScriptID: scriptA, ScriptSlug: "migrate-schema"},
			{ID: stepB, ScriptID: scriptA, ScriptSlug: "seed-database"},
		},
		"duplicate Script slug": {
			{ID: stepA, ScriptID: scriptA, ScriptSlug: "migrate-schema"},
			{ID: stepB, ScriptID: scriptB, ScriptSlug: "migrate-schema"},
		},
	}
	for name, steps := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateTaskSteps(steps); err == nil {
				t.Fatalf("validateTaskSteps(%#v) succeeded", steps)
			}
		})
	}
}
