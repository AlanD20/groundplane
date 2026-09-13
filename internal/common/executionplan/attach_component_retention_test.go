package executionplan

import (
	"crypto/sha256"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: standalone network mutations retain the Environment router alongside
// native workloads, including when the consumer is stopped and only config is checked.
func TestSealAttachRetainsUnselectedComponent(t *testing.T) {
	for _, operation := range []agentpb.PlanOperation{
		agentpb.PlanOperation_PLAN_OPERATION_ATTACH, agentpb.PlanOperation_PLAN_OPERATION_DETACH,
	} {
		for _, running := range []bool{true, false} {
			plan := attachWithRetainedComponentPlan(operation)
			if !running {
				plan.Steps[0].GetComposeApply().ServiceIds = nil
				plan.Artifacts[0].Services[1].ExpectedReplicas = 0
			}
			sealed, err := Seal(plan)
			if err != nil {
				t.Fatalf("Seal(%s, running=%t): %v", operation, running, err)
			}
			names, selected, err := AttachMutationServices(sealed, testStepID)
			var want []string
			if running {
				want = []string{"api--singleton"}
			}
			if err != nil || !selected || !slices.Equal(names, want) {
				t.Fatalf("runtime selection = %v, %t, %v; want %v", names, selected, err, want)
			}
		}
	}
}

// Rationale: retaining Component ownership must not authorize its startup,
// recreation, dependency expansion or lifecycle mutation through an Attach.
func TestSealAttachRejectsRetainedComponentMutation(t *testing.T) {
	for _, operation := range []agentpb.PlanOperation{
		agentpb.PlanOperation_PLAN_OPERATION_ATTACH, agentpb.PlanOperation_PLAN_OPERATION_DETACH,
	} {
		for _, name := range []string{"select-component", "dependencies", "full-reconcile", "force-recreate", "stop", "remove"} {
			t.Run(operation.String()+"/"+name, func(t *testing.T) {
				plan := attachWithRetainedComponentPlan(operation)
				apply := plan.Steps[0].GetComposeApply()
				componentID := plan.Artifacts[0].Services[0].ServiceId
				switch name {
				case "select-component":
					apply.ServiceIds = []string{componentID}
				case "dependencies":
					apply.NoDependencies = false
				case "full-reconcile":
					apply.FullReconcile = true
				case "force-recreate":
					apply.ForceRecreate = true
				case "stop":
					plan.Steps[0].Payload = &agentpb.ExecutionStep_ComposeStop{ComposeStop: &agentpb.ComposeStop{
						ArtifactId: testArtifact, ServiceIds: []string{componentID}, GraceSeconds: 10,
					}}
				case "remove":
					plan.Steps[0].Payload = &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
						ArtifactId: testArtifact, ServiceIds: []string{componentID},
					}}
				}
				if _, err := Seal(plan); err == nil {
					t.Fatal("sealed an Attach that mutates retained Component runtime")
				}
			})
		}
	}
}

func attachWithRetainedComponentPlan(operation agentpb.PlanOperation) *agentpb.ExecutionPlan {
	plan := validPlan()
	plan.Operation, plan.TargetId = operation, "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.PlanId, plan.RenderGeneration = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW", 8
	artifact := plan.Artifacts[0]
	artifact.OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	artifact.OwnerId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifact.ProjectName = "gp-" + strings.ToLower(artifact.OwnerId)
	artifact.AuthorizedVolumeDir = "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + artifact.OwnerId
	workload := artifact.Services[0]
	workload.ComposeName = "api--singleton"
	workload.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
	workload.ExpectedLabels = append([]*agentpb.LabelPair{{Key: labelEnvironmentID, Value: artifact.OwnerId}},
		workload.ExpectedLabels...)
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	component := &agentpb.ComposeService{
		ServiceId: componentID, ComposeName: "router", ExpectedReplicas: 1, OwnerComponentId: componentID,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: labelComponentID, Value: componentID},
			{Key: labelEnvironmentID, Value: artifact.OwnerId},
			{Key: labelKind, Value: "service"},
			{Key: labelManaged, Value: "true"},
			{Key: labelPlanID, Value: testPlanID},
			{Key: labelRenderGen, Value: "7"},
			{Key: labelServiceID, Value: componentID},
		},
	}
	bindTestComponentImageIdentity(component)
	artifact.Services = []*agentpb.ComposeService{component, workload}
	artifact.CanonicalYaml = []byte("services:\n  api--singleton:\n    image: registry.example/api@sha256:" +
		strings.Repeat("a", 64) + "\n  router:\n    image: " + component.ImageReference + "\n")
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	plan.Steps[0].GetComposeApply().NoDependencies = true
	return plan
}
