package executionplan

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestEnvironmentDirectoryCreatePlanSealsWithoutComposeArtifacts(t *testing.T) {
	sealed, err := Seal(validEnvironmentDirectoryPlan(
		"/srv/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + testEnvironmentID,
	))
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if err := AuthorizeVolumeDirectories(sealed, "/srv/groundplane/vol"); err != nil {
		t.Fatalf("AuthorizeVolumeDirectories() error = %v", err)
	}
}

func TestEnvironmentDirectoryCreatePlanRejectsMixedOrConfusedShape(t *testing.T) {
	tests := map[string]func(*agentpb.ExecutionPlan){
		"Compose artifact": func(plan *agentpb.ExecutionPlan) {
			plan.Artifacts = validPlan().Artifacts
		},
		"non-environment target": func(plan *agentpb.ExecutionPlan) {
			plan.TargetId = testServiceID
		},
		"payload target mismatch": func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetEnvironmentDirectoryCreate().EnvironmentId =
				"env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		"directory owner mismatch": func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetEnvironmentDirectoryCreate().ExpectedVolumeDir =
				"/srv/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			plan := validEnvironmentDirectoryPlan(
				"/srv/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + testEnvironmentID,
			)
			mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal() error = %v, want validation.failed", err)
			}
		})
	}
}

func TestEnvironmentDirectoryCreateAuthorizationRejectsDifferentTrustedRoot(t *testing.T) {
	sealed, err := Seal(validEnvironmentDirectoryPlan(
		"/srv/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + testEnvironmentID,
	))
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if err := AuthorizeVolumeDirectories(
		sealed,
		"/var/lib/groundplane/vol",
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("AuthorizeVolumeDirectories() error = %v, want validation.failed", err)
	}
}

func validEnvironmentDirectoryPlan(volumeDirectory string) *agentpb.ExecutionPlan {
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		TargetId:  testEnvironmentID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId: testEnvironmentID, ExpectedVolumeDir: volumeDirectory,
				},
			},
		}},
	}
}
