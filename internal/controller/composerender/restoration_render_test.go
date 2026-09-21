package composerender

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: the real blue-green renderer includes an uninstantiated opposite
// slot. That slot is neither another serving Release nor a corrupt singleton.
func TestRenderedBlueGreenLeavesOppositeSlotUninstantiated(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 39)
	releaseID := composeIdentityTestID(ids.KindDeployment, 40)
	input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{"api": {
		Image:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Expose: []string{"8080"}, HealthCheck: &composetypes.HealthCheckConfig{},
	}}})
	input.Identities.Services = []composeidentity.Resource{{ID: serviceID, Name: "api"}}
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
	serving, uninstantiated, proxies := 0, 0, 0
	for _, service := range artifact.Services {
		if service.ServiceId != serviceID {
			t.Fatal("renderer added an unrelated Service")
		}
		var recordedRelease string
		for _, label := range service.ExpectedLabels {
			if label.Key == "com.groundplane.release-id" {
				recordedRelease = label.Value
			}
		}
		switch service.Role {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
			proxies++
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
			switch {
			case service.Slot == "blue" && recordedRelease == releaseID:
				serving++
			case service.Slot == "green" && recordedRelease == "":
				uninstantiated++
			default:
				t.Fatal("rendered slot has unexpected Release ownership")
			}
		default:
			t.Fatal("renderer added an unexpected runtime role")
		}
	}
	if serving != 1 || uninstantiated != 1 || proxies != 1 {
		t.Fatalf("rendered roles = %d serving, %d uninstantiated, %d proxies", serving, uninstantiated, proxies)
	}
}
