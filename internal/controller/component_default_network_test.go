package controller

import (
	"runtime"
	"slices"
	"strings"
	"testing"

	component "github.com/AlanD20/groundplane-component-sdk/component"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Explicit Tunnel placement must preserve the selected egress gateway in the
// canonical Compose projection without adding an implicit default bridge.
func TestRegisteredTunnelRendersSelectedZoneGateway(t *testing.T) {
	serviceID, componentID, zoneID := ids.New(ids.KindService), ids.New(ids.KindComponent), ids.New(ids.KindNetwork)
	plan, err := registeredtunnel.Plan(registeredtunnel.Input{
		GeneratedServiceID: serviceID, SecretID: ids.New(ids.KindSecret),
		Zones: []component.NetworkInput{{ID: zoneID, Name: "frontend"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	platform, reference, found := registeredtunnel.Image.Select(runtime.GOOS, runtime.GOARCH)
	if !found {
		t.Fatal("unsupported platform")
	}
	image := SelectedComponentImage{
		Repository:  registeredtunnel.Image.Repository,
		IndexDigest: registeredtunnel.Image.IndexDigest,
		Reference:   reference,
		Platform:    platform,
	}
	project := &composetypes.Project{
		Services: composetypes.Services{}, Networks: composetypes.Networks{"frontend": {Name: "frontend"}},
	}
	input := composeRenderTestInput(project)
	environment := core.Environment{
		ID: input.EnvironmentID, VolumeDir: input.AuthorizedVolumeDir,
		Zones: map[string]core.Zone{"frontend": {ID: zoneID, Name: "frontend"}},
	}
	service, _, err := projectEnvironmentComponentService(
		environment,
		GeneratedEnvironmentService{
			ComponentID: componentID,
			Name:        registeredtunnel.ServiceName,
			Definition:  plan.Services[0],
		},
		image,
	)
	if err != nil {
		t.Fatal(err)
	}
	if service.NetworkMode != "" || len(service.Networks) != 1 ||
		service.Networks["frontend"].GatewayPriority != 1 {
		t.Fatalf("Tunnel networking = %q, %v; want selected Zone gateway", service.NetworkMode, service.Networks)
	}
	project.Services[registeredtunnel.ServiceName] = service
	input.Identities.Services = []ComposeResourceIdentity{
		{ID: serviceID, Name: registeredtunnel.ServiceName, ComponentID: componentID, ComponentImage: &image},
	}
	input.Identities.Networks = []ComposeResourceIdentity{{ID: zoneID, Name: "frontend"}}
	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.Networks) != 1 || !strings.Contains(string(artifact.CanonicalYaml), "gw_priority: 1") {
		t.Fatalf("render lost selected gateway networking: %s", artifact.CanonicalYaml)
	}
	if !artifact.Services[0].HasHealthcheck || service.HealthCheck == nil {
		t.Fatal("registered Tunnel cannot satisfy strict WaitHealthy: no rendered healthcheck")
	}
	if !slices.Equal(
		[]string(service.HealthCheck.Test),
		[]string{"CMD", "cloudflared", "tunnel", "--metrics", "127.0.0.1:2000", "ready"},
	) ||
		service.HealthCheck.Interval == nil ||
		service.HealthCheck.Timeout == nil ||
		service.HealthCheck.StartPeriod == nil ||
		service.HealthCheck.Retries == nil ||
		*service.HealthCheck.Retries != 3 {
		t.Fatal("projection lost exec-form readiness command or bounded timings")
	}
	service.HealthCheck.Test[1] = "mutated"
	if plan.Services[0].Healthcheck.Command[0] != "cloudflared" {
		t.Fatal("projected healthcheck aliases SDK plan")
	}
	plan.Services[0].Healthcheck.TimeoutSeconds = 0
	if _, _, err := projectEnvironmentComponentService(environment, GeneratedEnvironmentService{ComponentID: componentID, Name: registeredtunnel.ServiceName, Definition: plan.Services[0]}, image); err == nil {
		t.Fatal("projection accepted invalid healthcheck")
	}
}
