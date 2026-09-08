package executionplan

import (
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func blueprintComponentChainFixture() (*agentpb.ExecutionStep, map[string]*agentpb.ComposeArtifact, []*agentpb.ExecutionStep) {
	digest := sha256.Sum256([]byte("sealed component content"))
	action := &agentpb.ExecutionStep{
		StepId:             "action",
		PrerequisiteStepId: "health",
		Payload: &agentpb.ExecutionStep_ComponentApply{
			ComponentApply: &agentpb.ComponentApply{
				ComponentId:    "component",
				ArtifactId:     "content",
				ArtifactDigest: digest[:],
				Generation:     7,
			},
		},
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:          "compose",
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             "environment",
		AuthorizedVolumeDir: "/volumes",
		Services: []*agentpb.ComposeService{
			{
				ServiceId:        "service",
				OwnerComponentId: "component",
				ExpectedLabels:   []*agentpb.LabelPair{{Key: labelRenderGen, Value: "7"}},
			},
		},
	}
	materialize := &agentpb.ExecutionStep{
		StepId: "materialize",
		Payload: &agentpb.ExecutionStep_MaterializeFile{
			MaterializeFile: &agentpb.MaterializeFile{
				MaterializationId: "content",
				ArtifactId:        "compose",
				EnvironmentId:     "environment",
				Sha256:            digest[:],
			},
		},
	}
	apply := &agentpb.ExecutionStep{
		StepId:             "apply",
		PrerequisiteStepId: "materialize",
		Payload: &agentpb.ExecutionStep_ComposeApply{
			ComposeApply: &agentpb.ComposeApply{
				ArtifactId:     "compose",
				ServiceIds:     []string{"service"},
				ForceRecreate:  true,
				NoDependencies: true,
			},
		},
	}
	post := &agentpb.ExecutionStep{
		StepId:             "post",
		PrerequisiteStepId: "apply",
		Payload:            &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{}},
	}
	health := &agentpb.ExecutionStep{
		StepId:             "health",
		PrerequisiteStepId: "post",
		Payload: &agentpb.ExecutionStep_WaitHealthy{
			WaitHealthy: &agentpb.WaitHealthy{ArtifactId: "compose", ServiceIds: []string{"service"}},
		},
	}
	return action, map[string]*agentpb.ComposeArtifact{
			"compose": artifact,
		}, []*agentpb.ExecutionStep{
			materialize,
			apply,
			post,
			health,
			action,
		}
}

func TestBlueprintComponentIdentityThroughGlobalBarriers(t *testing.T) {
	action, artifacts, steps := blueprintComponentChainFixture()
	if err := validateComponentContainerActionTarget(agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, action.GetComponentApply(), action, artifacts, steps); err != nil {
		t.Fatal(err)
	}
	if err := validateComponentContainerActionTarget(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, action.GetComponentApply(), action, artifacts, steps); err == nil {
		t.Fatal("ordinary reconcile accepted non-immediate authority")
	}
	action.PrerequisiteStepId = "apply"
	if err := validateComponentContainerActionTarget(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, action.GetComponentApply(), action, artifacts, steps); err != nil {
		t.Fatal(err)
	}
}

func TestBlueprintComponentTargetBindsArtifactBeforeOwnershipCardinality(t *testing.T) {
	action, artifacts, steps := blueprintComponentChainFixture()
	previous := proto.CloneOf(artifacts["compose"])
	previous.ArtifactId = "previous"
	previous.Services[0].ServiceId = "previous-service"
	artifacts[previous.ArtifactId] = previous

	if err := validateComponentContainerActionTarget(
		agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		action.GetComponentApply(), action, artifacts, steps,
	); err != nil {
		t.Fatalf("Blueprint target should use the artifact bound by its materialization: %v", err)
	}

	action.PrerequisiteStepId = "apply"
	if err := validateComponentContainerActionTarget(
		agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		action.GetComponentApply(), action, artifacts, steps,
	); err == nil {
		t.Fatal("ordinary operation accepted duplicate Component ownership across artifacts")
	}
}

func TestBlueprintComponentTargetRejectsSelectedArtifactAmbiguityAndPriorOnly(t *testing.T) {
	t.Run("duplicate selected services", func(t *testing.T) {
		action, artifacts, steps := blueprintComponentChainFixture()
		artifacts["compose"].Services = append(
			artifacts["compose"].Services,
			proto.CloneOf(artifacts["compose"].Services[0]),
		)
		if err := validateComponentContainerActionTarget(
			agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			action.GetComponentApply(), action, artifacts, steps,
		); err == nil {
			t.Fatal("Blueprint target accepted duplicate ownership in its bound artifact")
		}
	})

	t.Run("prior only", func(t *testing.T) {
		action, artifacts, steps := blueprintComponentChainFixture()
		previous := proto.CloneOf(artifacts["compose"])
		previous.ArtifactId = "previous"
		artifacts = map[string]*agentpb.ComposeArtifact{"previous": previous}
		steps = append([]*agentpb.ExecutionStep(nil), steps[1:]...)
		if err := validateComponentContainerActionTarget(
			agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			action.GetComponentApply(), action, artifacts, steps,
		); err == nil {
			t.Fatal("Blueprint target accepted a prior artifact without its bound materialization")
		}
	})
}

func TestBlueprintComponentChainRejectsDetachedOrForgedAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ExecutionStep, map[string]*agentpb.ComposeArtifact, []*agentpb.ExecutionStep)
	}{
		{"content digest", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[0].GetMaterializeFile().Sha256 = []byte("forged")
		}},
		{"untargeted apply", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[1].GetComposeApply().NoDependencies = false
		}},
		{"duplicate content", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[2].Payload = proto.CloneOf(s[0]).Payload
		}},
		{"content owner", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[0].GetMaterializeFile().EnvironmentId = "foreign"
		}},
		{"content artifact", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[0].GetMaterializeFile().ArtifactId = "foreign"
		}},
		{"service owner", func(_ *agentpb.ExecutionStep, a map[string]*agentpb.ComposeArtifact, _ []*agentpb.ExecutionStep) {
			a["compose"].Services[0].OwnerComponentId = "foreign"
		}},
		{"duplicate target", func(_ *agentpb.ExecutionStep, a map[string]*agentpb.ComposeArtifact, _ []*agentpb.ExecutionStep) {
			a["compose"].Services = append(a["compose"].Services, proto.CloneOf(a["compose"].Services[0]))
		}},
		{"generation", func(a *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, _ []*agentpb.ExecutionStep) {
			a.GetComponentApply().Generation++
		}},
		{"detached content", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[1].PrerequisiteStepId = ""
		}},
		{"future barrier", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[2].PrerequisiteStepId = "health"
		}},
		{"self cycle", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[3].PrerequisiteStepId = "health"
		}},
		{"recovery barrier", func(_ *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, s []*agentpb.ExecutionStep) {
			s[2].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE
		}},
		{"host managed content", func(a *agentpb.ExecutionStep, _ map[string]*agentpb.ComposeArtifact, _ []*agentpb.ExecutionStep) {
			a.GetComponentApply().ManagedConfigContent = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, artifacts, steps := blueprintComponentChainFixture()
			test.mutate(a, artifacts, steps)
			if err := validateComponentContainerActionTarget(agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, a.GetComponentApply(), a, artifacts, steps); err == nil {
				t.Fatal("accepted invalid authority chain")
			}
		})
	}
}
