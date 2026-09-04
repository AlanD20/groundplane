package executionplan

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: disabling a serving Component removes the predecessor artifact,
// whose immutable labels identify its original plan rather than the new Task.
func TestSealAllowsComponentDisableToAuthenticateServiceFromPriorPlan(t *testing.T) {
	plan := priorOwnedComponentDisablePlan()

	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(Component disable using prior plan labels) error = %v", err)
	}
	if plan.GetComponentLifecycleMode() != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE {
		t.Fatal("test fixture lost Component disable lifecycle mode")
	}
}

// Rationale: predecessor labels are not generic compatibility authority; only
// the exact typed Component, artifact, service, and restore-before-remove
// procedure may authenticate them.
func TestSealRejectsInvalidPriorComponentDisableOwnership(t *testing.T) {
	t.Parallel()
	otherComponentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	otherServiceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{
			name: "wrong artifact kind",
			mutate: func(plan *agentpb.ExecutionPlan) {
				plan.Artifacts[0].OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_UNSPECIFIED
			},
		},
		{
			name: "wrong service count",
			mutate: func(plan *agentpb.ExecutionPlan) {
				plan.Artifacts[0].Services = append(
					plan.Artifacts[0].Services,
					proto.Clone(plan.Artifacts[0].Services[0]).(*agentpb.ComposeService),
				)
				plan.Artifacts[0].Services[1].ServiceId = otherServiceID
			},
		},
		{
			name: "wrong step order",
			mutate: func(plan *agentpb.ExecutionPlan) {
				remove := plan.Steps[1]
				restore := plan.Steps[0]
				remove.PrerequisiteStepId = ""
				restore.PrerequisiteStepId = remove.GetStepId()
				plan.Steps = []*agentpb.ExecutionStep{remove, restore}
			},
		},
		{
			name: "wrong selected service",
			mutate: func(plan *agentpb.ExecutionPlan) {
				plan.Steps[1].GetComposeRemove().ServiceIds[0] = otherServiceID
			},
		},
		{
			name: "wrong Component",
			mutate: func(plan *agentpb.ExecutionPlan) {
				plan.Steps[0].GetHostResolutionRestore().ComponentId = otherComponentID
			},
		},
		{
			name: "malformed prior plan label",
			mutate: func(plan *agentpb.ExecutionPlan) {
				setServiceLabel(plan.Artifacts[0].Services[0], labelPlanID, "not-a-plan")
			},
		},
		{
			name: "empty prior generation label",
			mutate: func(plan *agentpb.ExecutionPlan) {
				setServiceLabel(plan.Artifacts[0].Services[0], labelRenderGen, "")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := priorOwnedComponentDisablePlan()
			test.mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal(invalid prior-owned Component disable) error = %v, want validation.failed", err)
			}
		})
	}
}

func priorOwnedComponentDisablePlan() *agentpb.ExecutionPlan {
	plan := validComponentDisablePlan()
	plan.PlanId = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	plan.RenderGeneration = 8
	plan.ComponentRollbackObservation.Generation = plan.RenderGeneration
	plan.Steps[0].GetHostResolutionRestore().Generation = plan.RenderGeneration
	return plan
}

func setServiceLabel(service *agentpb.ComposeService, key string, value string) {
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == key {
			label.Value = value
			return
		}
	}
}
