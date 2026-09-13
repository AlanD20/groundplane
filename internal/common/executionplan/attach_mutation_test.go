package executionplan

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: selecting one logical Service for Attach must not start its proxy,
// inactive slot or an unrelated Service from the same captured artifact.
func TestAttachMutationServicesScopesActiveWorkloads(t *testing.T) {
	step := &agentpb.ExecutionStep{StepId: "step", Payload: &agentpb.ExecutionStep_ComposeApply{
		ComposeApply: &agentpb.ComposeApply{ArtifactId: "artifact", ServiceIds: []string{"api"}, NoDependencies: true},
	}}
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_ATTACH,
		Steps: []*agentpb.ExecutionStep{step}, Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: "artifact", Services: []*agentpb.ComposeService{
				{ServiceId: "api", ComposeName: "api--blue", ExpectedReplicas: 1,
					Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT},
				{ServiceId: "api", ComposeName: "api--green",
					Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT},
				{ServiceId: "api", ComposeName: "api", ExpectedReplicas: 1,
					Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY},
				{ServiceId: "worker", ComposeName: "worker--singleton", ExpectedReplicas: 1},
			},
		}}}
	names, selected, err := AttachMutationServices(plan, step.StepId)
	if err != nil || !selected || !slices.Equal(names, []string{"api--blue"}) {
		t.Fatalf("Attach selected %v, %t, %v", names, selected, err)
	}
	step.GetComposeApply().ServiceIds = nil
	names, selected, err = AttachMutationServices(plan, step.StepId)
	if err != nil || !selected || len(names) != 0 {
		t.Fatalf("stopped Attach selected %v, %t, %v", names, selected, err)
	}
	step.GetComposeApply().NoDependencies = false
	if _, _, err := AttachMutationServices(plan, step.StepId); err == nil {
		t.Fatal("Attach accepted dependency expansion")
	}
}
