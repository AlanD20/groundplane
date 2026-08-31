package executionplan

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	testPlanID    = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testArtifact  = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testStepID    = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: a Component action must not use caller-supplied service selectors
// to activate configuration in a service owned by another Component.
func TestSealRejectsComponentActionAgainstForeignOwnedService(t *testing.T) {
	plan := validPlan()
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	foreignComponentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	plan.TargetId = environmentID
	plan.Artifacts[0].OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	plan.Artifacts[0].OwnerId = environmentID
	plan.Artifacts[0].ProjectName = "gp-" + strings.ToLower(environmentID)
	plan.Artifacts[0].AuthorizedVolumeDir = "/var/lib/groundplane/volumes/" +
		"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID
	plan.Artifacts[0].Services[0].ExpectedLabels = []*agentpb.LabelPair{
		{Key: "com.groundplane.component-id", Value: foreignComponentID},
		{Key: labelEnvironmentID, Value: environmentID},
		{Key: labelKind, Value: "service"},
		{Key: labelManaged, Value: "true"},
		{Key: labelPlanID, Value: testPlanID},
		{Key: labelRenderGen, Value: "7"},
		{Key: labelServiceID, Value: testServiceID},
	}
	plan.Artifacts[0].Services[0].OwnerComponentId = foreignComponentID
	digest := sha256.Sum256([]byte("component artifact"))
	plan.Steps[0].Payload = &agentpb.ExecutionStep_ComponentApply{
		ComponentApply: &agentpb.ComponentApply{
			ComponentId: componentID, DefinitionDigest: digest[:], CatalogDigest: digest[:],
			ActionId: "activate-config", ArtifactId: testArtifact, ArtifactDigest: digest[:],
			Generation: 7,
		},
	}

	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(foreign Component service) error = %v, want validation.failed", err)
	}
}

func TestSealRejectsStaleComponentActionGeneration(t *testing.T) {
	plan := validPlan()
	digest := sha256.Sum256([]byte("component artifact"))
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	plan.TargetId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.Artifacts[0].OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	plan.Artifacts[0].OwnerId = plan.TargetId
	plan.Artifacts[0].AuthorizedVolumeDir = "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + plan.TargetId
	plan.Artifacts[0].Services[0].OwnerComponentId = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.Artifacts[0].Services[0].ExpectedLabels[0] = &agentpb.LabelPair{Key: labelComponentID, Value: plan.Artifacts[0].Services[0].OwnerComponentId}
	plan.Steps[0].Payload = &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
		ComponentId:      plan.Artifacts[0].Services[0].OwnerComponentId,
		DefinitionDigest: digest[:], CatalogDigest: digest[:], ActionId: "activate-config",
		ArtifactId: testArtifact, ArtifactDigest: digest[:], Generation: plan.RenderGeneration - 1,
	}}
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(stale Component generation) error = %v", err)
	}
}

func TestSealBindsRemoveComponentActionToSelectedServiceGeneration(t *testing.T) {
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	actionStepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	plan := validMaterializationPlan()
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	plan.RenderGeneration = 2
	plan.Artifacts[0].Services = []*agentpb.ComposeService{{
		ServiceId: testServiceID, ComposeName: "caddy", ExpectedReplicas: 1,
		OwnerComponentId: componentID,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: labelComponentID, Value: componentID},
			{Key: labelEnvironmentID, Value: materializationEnvironmentID},
			{Key: labelKind, Value: "service"},
			{Key: labelManaged, Value: "true"},
			{Key: labelPlanID, Value: materializationPlanID},
			{Key: labelRenderGen, Value: "2"},
			{Key: labelServiceID, Value: testServiceID},
		},
	}}
	materialization := plan.Steps[0].GetMaterializeFile()
	digest := append([]byte(nil), materialization.GetSha256()...)
	plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{
		StepId: actionStepID, TimeoutSeconds: 30, PrerequisiteStepId: materializationStepID,
		Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
			ComponentId: componentID, DefinitionDigest: digest, CatalogDigest: digest,
			ActionId: "activate-config", ArtifactId: materializationID,
			ArtifactDigest: append([]byte(nil), digest...), Generation: 2,
		}},
	})
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(current REMOVE Component generation) error = %v", err)
	}
	plan.Artifacts[0].Services[0].ExpectedLabels[5].Value = "1"
	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(stale REMOVE Component Service generation) error = %v", err)
	}
}

// Rationale: a sealed CoreDNS disable must make host resolution safe before
// stopping the resolver, while preserving a dependency that supports retry.
func TestSealAcceptsComponentDisableRestoreBeforeRemove(t *testing.T) {
	if _, err := Seal(validComponentDisablePlan()); err != nil {
		t.Fatalf("Seal(restore-before-remove Component disable) error = %v", err)
	}
}

// Rationale: exact plan validation must reject the crash-unsafe historical
// order even when both individual steps and their dependency are well formed.
func TestSealRejectsComponentDisableRemoveBeforeRestore(t *testing.T) {
	plan := validComponentDisablePlan()
	remove := plan.Steps[1]
	restore := plan.Steps[0]
	remove.PrerequisiteStepId = ""
	restore.PrerequisiteStepId = remove.GetStepId()
	plan.Steps = []*agentpb.ExecutionStep{remove, restore}

	if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Seal(remove-before-restore Component disable) error = %v, want validation.failed", err)
	}
}

// Rationale: the sealed four-step procedure means the generated Service must
// be applied, not that the lifecycle is necessarily a first enable. Serving
// predecessor evidence must select update compensation and remain mandatory.
func TestSealDistinguishesComponentServiceReapplyFromFirstEnable(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		mode        agentpb.ComponentLifecycleMode
		predecessor bool
		wantError   bool
	}{
		{
			name: "first enable", mode: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
		},
		{
			name: "serving update", mode: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE,
			predecessor: true,
		},
		{
			name: "enable cannot replace predecessor",
			mode: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE, predecessor: true, wantError: true,
		},
		{
			name: "update requires predecessor",
			mode: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE, wantError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Seal(validComponentServiceEnsurePlan(test.mode, test.predecessor))
			if (err != nil) != test.wantError {
				t.Fatalf("Seal() error = %v, wantError = %t", err, test.wantError)
			}
		})
	}
}

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

func validComponentDisablePlan() *agentpb.ExecutionPlan {
	plan := validPlan()
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	removeStepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY
	plan.TargetId = componentID
	plan.ComponentLifecycleMode = agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE
	digest := sha256.Sum256([]byte("resolver artifact"))
	plan.ComponentRollbackObservation = &agentpb.ComponentApply{
		ComponentId: componentID, DefinitionDigest: digest[:], CatalogDigest: digest[:],
		ActionId: "observe-serving", ArtifactId: testArtifact, ArtifactDigest: digest[:],
		Generation: plan.RenderGeneration,
	}
	plan.Steps = []*agentpb.ExecutionStep{
		{
			StepId: testStepID, TimeoutSeconds: 300,
			Payload: &agentpb.ExecutionStep_HostResolutionRestore{
				HostResolutionRestore: &agentpb.HostResolutionRestore{
					ComponentId: componentID, Generation: plan.RenderGeneration,
				},
			},
		},
		{
			StepId: removeStepID, TimeoutSeconds: 300, PrerequisiteStepId: testStepID,
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: testArtifact, ServiceIds: []string{testServiceID},
			}},
		},
	}
	return plan
}

func validComponentServiceEnsurePlan(
	mode agentpb.ComponentLifecycleMode,
	predecessor bool,
) *agentpb.ExecutionPlan {
	plan := validPlan()
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	digest := sha256.Sum256([]byte("resolver artifact"))
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY
	plan.TargetId = componentID
	plan.ComponentLifecycleMode = mode
	plan.Artifacts[0].Services[0].HasHealthcheck = false
	managed := &agentpb.ComponentApply{
		ComponentId: componentID, DefinitionDigest: digest[:], CatalogDigest: digest[:],
		ActionId: "activate-config", ArtifactId: testArtifact, ArtifactDigest: digest[:],
		Generation: plan.RenderGeneration, ManagedConfigContent: true,
	}
	if predecessor {
		managed.ExpectedPreviousArtifactDigest = append([]byte(nil), digest[:]...)
		managed.ExpectedPreviousArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		managed.ExpectedPreviousGeneration = 6
		rollback := proto.Clone(plan.Artifacts[0]).(*agentpb.ComposeArtifact)
		rollback.ArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		for _, label := range rollback.Services[0].ExpectedLabels {
			switch label.GetKey() {
			case labelPlanID:
				label.Value = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			case labelRenderGen:
				label.Value = "6"
			}
		}
		plan.Artifacts = append(plan.Artifacts, rollback)
	}
	plan.Steps = []*agentpb.ExecutionStep{
		{
			StepId: testStepID, TimeoutSeconds: 300,
			Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: managed},
		},
		{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", TimeoutSeconds: 300,
			PrerequisiteStepId: testStepID,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: testArtifact, ServiceIds: []string{testServiceID},
			}},
		},
		{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAX", TimeoutSeconds: 300,
			PrerequisiteStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
				ComponentId: componentID, DefinitionDigest: digest[:], CatalogDigest: digest[:],
				ActionId: "observe-serving", ArtifactId: testArtifact, ArtifactDigest: digest[:],
				Generation: plan.RenderGeneration,
			}},
		},
		{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAY", TimeoutSeconds: 300,
			PrerequisiteStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAX",
			Payload: &agentpb.ExecutionStep_HostResolutionApply{HostResolutionApply: &agentpb.HostResolutionApply{
				ComponentId: componentID, Generation: plan.RenderGeneration,
			}},
		},
	}
	return plan
}
