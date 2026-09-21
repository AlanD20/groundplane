package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestTaskStepKindRejectsMissingOrUnclosedVariant(t *testing.T) {
	// Rationale: schema-1 Task records must reject pre-discriminator and corrupt
	// RunScript steps instead of silently projecting them as ordinary work.
	stepID := ids.NewAt(ids.KindStep, time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC), 90)
	for name, step := range map[string]testtaskjournal.TaskStepRecord{
		"missing kind":     {ID: stepID},
		"missing identity": {Kind: testtaskjournal.TaskStepScript, ID: stepID},
	} {
		t.Run(name, func(t *testing.T) {
			if err := testtaskjournal.ValidateTaskSteps([]testtaskjournal.TaskStepRecord{step}); err == nil {
				t.Fatalf("validateTaskSteps(%#v) succeeded", step)
			}
		})
	}
}
