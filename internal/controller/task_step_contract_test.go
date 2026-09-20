package controller

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
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
