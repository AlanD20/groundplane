package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: the real blue-green renderer includes an uninstantiated opposite
// slot. That slot is neither another serving Release nor a corrupt singleton.
func TestRenderedBlueGreenRestorationSelectsAcknowledgedMember(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 39)
	releaseID := composeIdentityTestID(ids.KindDeployment, 40)
	input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{"api": {
		Image:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Expose: []string{"8080"}, HealthCheck: &composetypes.HealthCheckConfig{},
	}}})
	input.Identities.Services = []ComposeResourceIdentity{{ID: serviceID, Name: "api"}}
	input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
		ReleaseID: releaseID, Image: input.Project.Services["api"].Image,
		ProxyImage: testServiceProxyImage(), Strategy: domain.StrategyBlueGreen,
		Target: domain.WorkloadBlue, ServingTarget: domain.WorkloadBlue,
		ServingReleaseID: releaseID, ServingProxyGeneration: 1,
	}}
	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := executionplan.SelectBlueprintRestorationMembers([]executionplan.CandidateServiceIdentity{{
		ServiceID: serviceID, ReleaseID: composeIdentityTestID(ids.KindDeployment, 42),
	}}, artifact)
	if err != nil || len(selected) != 1 ||
		selected[0].Target != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
		t.Fatalf("rendered restoration selection = %v, %v", selected, err)
	}
}
