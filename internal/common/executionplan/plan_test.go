package executionplan

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	testPlanID    = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testArtifact  = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testStepID    = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: both channel endpoints must derive one immutable digest from the
// same complete typed plan and must own their returned message.
func TestSealAndValidateDeterministicPlan(t *testing.T) {
	plan := validPlan()
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if len(plan.PlanHash) != 0 || len(sealed.PlanHash) != sha256.Size {
		t.Fatalf("hash ownership: input=%x sealed=%x", plan.PlanHash, sealed.PlanHash)
	}
	validated, err := Validate(sealed)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	validated.Artifacts[0].CanonicalYaml[0] = 'X'
	if sealed.Artifacts[0].CanonicalYaml[0] == 'X' {
		t.Fatal("validated plan aliases the caller's protobuf")
	}
}

// Rationale: a valid digest authenticates the complete procedure, not only
// the plan identity fields.
func TestValidateRejectsPlanContentChangedAfterSealing(t *testing.T) {
	sealed, err := Seal(validPlan())
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	sealed.Steps[0].TimeoutSeconds++
	if _, err := Validate(sealed); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Validate(tampered) error = %v, want validation.failed", err)
	}
}

// Rationale: protobuf forward fields cannot silently change a version-one
// plan's hash or execution meaning.
func TestSealRejectsUnknownProtobufFields(t *testing.T) {
	plan := validPlan()
	plan.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(unknown field) error = %v, want validation.failed", err)
	}
}

// Rationale: a targeted mutation must resolve every selected stable service
// id through the authenticated artifact table.
func TestSealRejectsUnknownSelectedService(t *testing.T) {
	plan := validPlan()
	plan.Steps[0].GetComposeApply().ServiceIds[0] = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(unknown service) error = %v, want validation.failed", err)
	}
}

func TestSealAllowsRemoveToAuthenticateServiceFromPriorPlan(t *testing.T) {
	plan := validPlan()
	plan.PlanId = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	plan.RenderGeneration = 1
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	plan.TargetId = plan.Artifacts[0].ArtifactId
	plan.Steps = []*agentpb.ExecutionStep{{
		StepId: testStepID, TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
			ArtifactId: plan.Artifacts[0].ArtifactId, WholeProject: true,
		}},
	}}
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(remove using prior plan labels) error = %v", err)
	}
}

func validPlan() *agentpb.ExecutionPlan {
	yaml := []byte("services:\n  api:\n    image: registry.example/api@sha256:" + strings.Repeat("a", 64) + "\n")
	yamlDigest := sha256.Sum256(yaml)
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: testServiceID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: testArtifact, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: testServiceID, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: labelKind, Value: "service"},
					{Key: labelManaged, Value: "true"},
					{Key: labelPlanID, Value: testPlanID},
					{Key: labelRenderGen, Value: "7"},
					{Key: labelServiceID, Value: testServiceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: testArtifact, ServiceIds: []string{testServiceID},
			}},
		}},
	}
}
