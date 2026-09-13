package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// The ordinary Attach repository fixtures create configured-only Services,
// without a serving Release. They validate Compose but cannot start a workload
// or invent native runtime authority. Running-native fixtures are separate.
func configuredAttachRuntimePreparation(task TaskRecord, environmentID string) *serviceruntimerecord.AttachPreparation {
	return &serviceruntimerecord.AttachPreparation{EnvironmentID: environmentID, PlanID: task.PlanID,
		PlanHash: task.PlanHash, StepID: task.Steps[0].ID, RenderGeneration: uint64(task.RenderGeneration)}
}

// QA: ATT-09, ATT-10; local publication validation.
// Rationale: a missing preparation or a desired-only running selection must be
// rejected before publication; configured-only validation needs no runtime.
func TestAttachRuntimePreparationRequiresExplicitSelectedAuthority(t *testing.T) {
	task := validTaskRecord(testAttachTime)
	input := capturedAttachRenderInputFixture(t)
	task.PlanID, task.RenderGeneration = input.PlanID, int32(input.RenderGeneration)
	if err := validateAttachRuntimePreparation(input, task); err == nil {
		t.Fatal("missing preparation accepted")
	}
	input.RuntimePreparation = configuredAttachRuntimePreparation(task, input.EnvironmentID)
	if err := validateAttachRuntimePreparation(input, task); err == nil {
		t.Fatal("running Service without acknowledged update accepted")
	}
	input.RunningServiceIDs = nil
	if err := validateAttachRuntimePreparation(input, task); err != nil {
		t.Fatal(err)
	}
	task.PlanHash = "changed"
	if err := validateAttachRuntimePreparation(input, task); err == nil {
		t.Fatal("changed plan accepted")
	}
}
