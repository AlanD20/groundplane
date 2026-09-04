package agent

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: automatic reconciliation is accepted only for the closed
// Component Apply procedure; a different operation must fail before enqueue.
func TestAutomaticReconcileAssignmentRequiresComponentApply(t *testing.T) {
	t.Parallel()

	component := Assignment{
		AssignmentID:       workerTestAssignmentID,
		TaskID:             workerTestTaskID,
		OperationID:        "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan:               automaticComponentApplyPlan(t),
		AutomaticReconcile: true,
		ExecutionEpoch:     1,
		ExecutionMode:      agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline:    time.Unix(1_700_000_000, 0).UTC().Add(time.Minute),
		RecoveryDeadline:   time.Unix(1_700_000_000, 0).UTC().Add(2 * time.Minute),
	}
	accepted, err := validateAndCopyAssignment(component, "/var/lib/groundplane/vol")
	if err != nil {
		t.Fatalf("validateAndCopyAssignment(component apply) error = %v", err)
	}
	if !accepted.AutomaticReconcile ||
		accepted.Plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		t.Fatalf("accepted automatic assignment = %#v", accepted)
	}

	foreign := workerAssignment(workerOtherTaskID, "plan-a")
	foreign.AutomaticReconcile = true
	if _, err := validateAndCopyAssignment(foreign, "/var/lib/groundplane/vol"); err == nil {
		t.Fatal("validateAndCopyAssignment(deploy) accepted automatic reconciliation")
	}
}

func automaticComponentApplyPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	const (
		componentID   = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		planID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		applyStepID   = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		observeStepID = "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	)
	digest := sha256.Sum256([]byte("CoreDNS component action"))
	yaml := []byte("services:\n  coredns:\n    image: registry.example/coredns@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n")
	yamlDigest := sha256.Sum256(yaml)
	plan := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, TargetId: componentID,
		ComponentLifecycleMode: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: serviceID, ComposeName: "coredns", HasHealthcheck: false,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: planID},
					{Key: "com.groundplane.render-generation", Value: "1"},
					{Key: "com.groundplane.service-id", Value: serviceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{
			{
				StepId: applyStepID, TimeoutSeconds: 30,
				Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
					ComponentId: componentID, DefinitionDigest: digest[:], CatalogDigest: digest[:],
					ActionId: "activate-config", ArtifactId: artifactID, ArtifactDigest: digest[:],
					Generation: 1, ManagedConfigContent: true,
				}},
			},
			{
				StepId: observeStepID, TimeoutSeconds: 30, PrerequisiteStepId: applyStepID,
				Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
					ComponentId: componentID, DefinitionDigest: digest[:], CatalogDigest: digest[:],
					ActionId: "observe-serving", ArtifactId: artifactID, ArtifactDigest: digest[:],
					Generation: 1,
				}},
			},
		},
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		t.Fatalf("executionplan.Seal() error = %v", err)
	}
	return sealed
}
