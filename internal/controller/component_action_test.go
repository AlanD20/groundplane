package controller

import (
	"crypto/sha256"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: applying a Service is not equivalent to first enable. An update
// with a serving predecessor must retain update compensation even when the
// sealed procedure has to reapply the generated Service.
func TestComponentActionLifecycleModeUsesPredecessorEvidence(t *testing.T) {
	t.Parallel()
	previous := make([]byte, sha256.Size)
	rollback := &agentpb.ComposeArtifact{}
	for _, test := range []struct {
		name          string
		ensureService bool
		rollback      *agentpb.ComposeArtifact
		previous      []byte
		want          agentpb.ComponentLifecycleMode
		wantError     bool
	}{
		{
			name: "first enable", ensureService: true,
			want: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
		},
		{
			name: "update reapplies service", ensureService: true, rollback: rollback, previous: previous,
			want: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE,
		},
		{
			name: "in-place update", previous: previous,
			want: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE,
		},
		{name: "invalid predecessor", ensureService: true, previous: []byte{1}, wantError: true},
		{name: "rollback without service apply", rollback: rollback, previous: previous, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := componentActionLifecycleMode(test.ensureService, test.rollback, test.previous)
			if (err != nil) != test.wantError || got != test.want {
				t.Fatalf("componentActionLifecycleMode() = %s, %v", got, err)
			}
		})
	}
}

// Rationale: host resolution must be restored before CoreDNS stops so a crash
// or retry after Compose removal cannot leave the host using a dead resolver.
func TestBuildComponentDisableExecutionPlanRestoresHostBeforeRemovingService(t *testing.T) {
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	planID := "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	image, platform, imageReference := controllerTestOCIPlatform("example/resolver")
	artifact, err := RenderPlatformComponentCompose(PlatformComponentComposeInput{
		ComponentID: componentID, PlanID: planID, RenderGeneration: 7,
		ArtifactID:      "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ImageRepository: image.Repository, ImageIndexDigest: image.IndexDigest,
		ImageConfigDigest: platform.ConfigDigest,
		ImageChildDigest:  platform.ChildDigest, ImageReference: imageReference,
		ImageOS: platform.OS, ImageArchitecture: platform.Architecture, ImageVariant: platform.Variant,
		Plan: componentsdk.EnvironmentPlan{Services: []componentsdk.ManagedService{{
			ID: serviceID, Name: "resolver",
			Image:       image,
			NetworkMode: componentsdk.ManagedNetworkModeHost,
			Command:     []string{"-conf", "/etc/resolver/config"}, Restart: "unless-stopped", Replicas: 1,
			Mounts: []componentsdk.ManagedMount{{
				Kind: componentsdk.ManagedMountKindDirectory, Source: "config", Target: "/etc/resolver", ReadOnly: true,
			}},
			ObservationAction: "observe-serving",
		}}, Files: []componentsdk.ManagedFile{{Path: "config/config", Content: []byte(".:53 {}\n")}}},
	})
	if err != nil {
		t.Fatalf("RenderPlatformComponentCompose() error = %v", err)
	}
	definitionDigest := sha256.Sum256([]byte("definition"))
	catalogDigest := sha256.Sum256([]byte("catalog"))
	artifactDigest := sha256.Sum256([]byte(".:53 {}\n"))
	componentRef, componentErr := componentsdk.NewComponentID(componentID)
	if componentErr != nil {
		t.Fatal(componentErr)
	}
	artifactID, artifactErr := componentsdk.NewArtifactID("cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if artifactErr != nil {
		t.Fatal(artifactErr)
	}
	artifactRef, artifactRefErr := componentsdk.NewArtifactReference(artifactID, artifactDigest)
	if artifactRefErr != nil {
		t.Fatal(artifactRefErr)
	}
	envelope, envelopeErr := componentsdk.NewActionEnvelope(componentsdk.ActionEnvelopeInput{
		ComponentID: componentRef, DefinitionDigest: definitionDigest, CatalogDigest: catalogDigest,
		ActionID: "activate-config", Artifact: artifactRef, Generation: 7,
	})
	if envelopeErr != nil {
		t.Fatal(envelopeErr)
	}
	plan, err := BuildComponentDisableExecutionPlan(ComponentDisablePlanInput{
		VolumeRoot: "/var/lib/groundplane", Envelope: envelope, PlanID: planID,
		StepIDs:          []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAV", "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
		RenderGeneration: 7, ComposeArtifact: artifact, ObservationAction: "observe-serving",
		ExpectedPreviousArtifactDigest: artifactDigest[:],
	})
	if err != nil {
		t.Fatalf("BuildComponentDisableExecutionPlan() error = %v", err)
	}
	if len(plan.GetSteps()) != 2 || plan.GetSteps()[0].GetHostResolutionRestore() == nil ||
		plan.GetSteps()[1].GetComposeRemove() == nil ||
		plan.GetSteps()[1].GetPrerequisiteStepId() != plan.GetSteps()[0].GetStepId() {
		t.Fatalf("disable procedure = %#v", plan.GetSteps())
	}
	if plan.GetComponentRollbackObservation() == nil {
		t.Fatal("disable procedure omitted rollback observation action")
	}
	if artifact.GetServices()[0].GetHasHealthcheck() {
		t.Fatal("platform DNS resolver Compose artifact retained healthcheck authority")
	}
}
