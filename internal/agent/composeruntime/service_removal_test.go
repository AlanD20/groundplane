package composeruntime

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SVC-04: completed Docker commands are not removal proof. Remaining or foreign
// members must block completion, while a later captured slot can await its step.
func TestServiceRemovalRequiresPhysicalAbsence(t *testing.T) {
	for _, test := range []struct {
		name                                        string
		collisionName, collisionService             string
		remaining, final, wantSuccess, wantMutation bool
	}{
		{name: "absent", wantSuccess: true, wantMutation: true},
		{name: "future slot", collisionName: "api--green", collisionService: "svc_api", wantSuccess: true, wantMutation: true},
		{name: "unknown owned member", collisionName: "unexpected", collisionService: "svc_api"},
		{name: "foreign selected name", collisionName: "api--blue", collisionService: "svc_other"},
		{name: "prior member reappears", collisionName: "api--blue", collisionService: "svc_api", final: true},
		{name: "Docker left container", remaining: true, wantMutation: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
				ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{
					Sources: []*agentpb.ServiceLifecycleSource{
						{
							ArtifactId:   "blue",
							ServiceId:    "svc_api",
							ComposeNames: []string{"api--blue"},
							StepId:       "first",
						},
						{
							ArtifactId:   "green",
							ServiceId:    "svc_api",
							ComposeNames: []string{"api--green"},
							StepId:       "last",
						},
					},
				},
				Artifacts: []*agentpb.ComposeArtifact{{ArtifactId: "blue"}, {ArtifactId: "green"}},
			}
			index := 0
			if test.final {
				index = 1
			}
			source, artifact := plan.ServiceLifecycleProcedure.Sources[index], plan.Artifacts[index]
			step := &agentpb.ExecutionStep{StepId: source.StepId, TimeoutSeconds: 30,
				Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
					ArtifactId: artifact.ArtifactId, ServiceIds: []string{"svc_api"},
				}},
			}
			observed := &agentpb.ObservedProject{}
			if test.collisionName != "" {
				observed.Collisions = []*agentpb.ObservedCollision{{
					Kind:      agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
					ServiceId: test.collisionService, ComposeServiceName: test.collisionName,
				}}
			}
			if test.remaining {
				observed.Containers = []*agentpb.ObservedContainer{{ServiceId: "svc_api"}}
			}
			helper := completedComposeHelper()
			runtime, err := New(
				helper,
				&recreateArtifactObserver{projects: map[string]*agentpb.ObservedProject{artifact.ArtifactId: observed}},
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = runtime.ExecuteStep(context.Background(), taskassignment.Assignment{Plan: plan}, step)
			if (err == nil) != test.wantSuccess || (helper.request != nil) != test.wantMutation {
				t.Fatalf("success=%t mutation=%t error=%v", err == nil, helper.request != nil, err)
			}
		})
	}
}
