package agent

import (
	"errors"
	"testing"

	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: exact Blueprint reapply has no candidate Release procedure, but
// still health-checks generated Components beside retained native runtimes.
// Only unrelated named collisions can be excluded from that selected health.
func TestBlueprintManagedHealthWithoutCandidateReleaseScopesRetainedRuntime(t *testing.T) {
	for _, scenario := range []string{
		"retained native", "selected name", "selected identity", "unnamed container",
		"selected network", "volume", "native selection", "non-Blueprint",
	} {
		t.Run(scenario, func(t *testing.T) {
			assignment, _ := composeRuntimeAssignment()
			plan := assignment.Plan
			plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
			artifact := plan.Artifacts[0]
			artifact.Services[0].OwnerComponentId = "cmp_router"
			artifact.Networks = []*agentpb.ComposeNetwork{{ComposeName: "frontend", DockerName: "gp_frontend"}}
			collision := &agentpb.ObservedCollision{
				Kind:               agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
				ComposeServiceName: "workload--blue", ServiceId: "svc_workload",
			}
			switch scenario {
			case "selected name":
				collision.ComposeServiceName = "api"
			case "selected identity":
				collision.ServiceId = "svc_api"
			case "unnamed container":
				collision.ComposeServiceName = ""
			case "selected network":
				collision.Kind = agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK
				collision.Name = "gp_frontend"
			case "volume":
				collision.Kind = agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME
			case "native selection":
				artifact.Services[0].OwnerComponentId = ""
			case "non-Blueprint":
				plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
			}
			healthy := &agentpb.ObservedProject{ProjectName: artifact.ProjectName,
				Containers: []*agentpb.ObservedContainer{healthyContainer("router-1", "svc_api")},
				Collisions: []*agentpb.ObservedCollision{collision},
			}
			warming := proto.CloneOf(healthy)
			warming.Containers[0].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING
			observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{warming, healthy}}
			helper := completedComposeHelper()
			runtime, err := testcomposeruntime.New(helper, observer)
			if err != nil {
				t.Fatal(err)
			}
			step := &agentpb.ExecutionStep{
				Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
					ArtifactId: artifact.ArtifactId, ServiceIds: []string{"svc_api"},
				}},
			}
			result, err := runtime.ExecuteStep(t.Context(), assignment, step)
			if scenario == "retained native" {
				if err != nil || result.ReconciliationRequired || observer.calls != 2 {
					t.Fatalf(
						"selected Component failed convergence beside retained native: %v, calls=%d",
						err,
						observer.calls,
					)
				}
			} else if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !result.ReconciliationRequired {
				t.Fatalf("ownership collision was not rejected: result=%v, error=%v", result, err)
			}
			if helper.request != nil || len(healthy.Collisions) != 1 {
				t.Fatal("health mutated workloads or discarded original collision evidence")
			}
		})
	}
}
