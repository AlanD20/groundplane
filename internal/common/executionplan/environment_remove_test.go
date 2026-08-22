package executionplan

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: destructive host work must bind the stable Environment id to one authorized directory beneath the
// configured volume root without requiring a Compose artifact to exist.
func TestEnvironmentDirectoryRemovePlanBindsTargetAndPath(t *testing.T) {
	const (
		root          = "/srv/groundplane/vol"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		directory     = root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID
	)
	plan, err := Seal(&agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetId: environmentID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
				EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{
					EnvironmentId: environmentID, ExpectedVolumeDir: directory,
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if err := AuthorizeVolumeDirectories(plan, root); err != nil {
		t.Fatalf("AuthorizeVolumeDirectories() error = %v", err)
	}
	plan.Steps[0].GetEnvironmentDirectoryRemove().ExpectedVolumeDir = root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := AuthorizeVolumeDirectories(plan, root); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("AuthorizeVolumeDirectories(mismatch) error = %v, want validation failure", err)
	}
}
