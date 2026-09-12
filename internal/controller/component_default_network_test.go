package controller

import (
	"runtime"
	"slices"
	"strings"
	"testing"

	component "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Explicit Component Service placement must preserve the selected egress gateway in the
// canonical Compose projection without adding an implicit default bridge.
func TestComponentServiceRendersSelectedZoneGateway(t *testing.T) {
	serviceID, componentID, zoneID := ids.New(ids.KindService), ids.New(ids.KindComponent), ids.New(ids.KindNetwork)
	definition := component.ManagedService{
		ID:          serviceID,
		Name:        testComponentServiceName,
		Image:       componentServiceTestImage(),
		NetworkMode: component.ManagedNetworkModeZones,
		Command:     []string{"tunnel", "--no-autoupdate", "--metrics", "127.0.0.1:2000", "run"},
		Networks: []component.ManagedNetworkAttachment{
			{Name: "frontend", GatewayPriority: 1},
		},
		Healthcheck: &component.ManagedHealthcheck{
			Command:         []string{"cloudflared", "tunnel", "--metrics", "127.0.0.1:2000", "ready"},
			IntervalSeconds: 5, TimeoutSeconds: 3, StartPeriodSeconds: 10, Retries: 3,
		},
		Restart:  "unless-stopped",
		Replicas: 1,
		SecretEnvironment: []component.ManagedSecretEnvironment{
			{Name: "TUNNEL_TOKEN", SecretID: ids.New(ids.KindSecret)},
		},
	}
	platform, reference, found := definition.Image.Select(runtime.GOOS, runtime.GOARCH)
	if !found {
		t.Fatal("unsupported platform")
	}
	image := SelectedComponentImage{
		Repository:  definition.Image.Repository,
		IndexDigest: definition.Image.IndexDigest,
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
			Name:        testComponentServiceName,
			Definition:  definition,
		},
		image,
	)
	if err != nil {
		t.Fatal(err)
	}
	if service.NetworkMode != "" || len(service.Networks) != 1 ||
		service.Networks["frontend"].GatewayPriority != 1 {
		t.Fatalf(
			"Component Service networking = %q, %v; want selected Zone gateway",
			service.NetworkMode,
			service.Networks,
		)
	}
	project.Services[testComponentServiceName] = service
	input.Identities.Services = []ComposeResourceIdentity{
		{ID: serviceID, Name: testComponentServiceName, ComponentID: componentID, ComponentImage: &image},
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
		t.Fatal("Component Service cannot satisfy strict WaitHealthy: no rendered healthcheck")
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
	if definition.Healthcheck.Command[0] != "cloudflared" {
		t.Fatal("projected healthcheck aliases SDK plan")
	}
	definition.Healthcheck.TimeoutSeconds = 0
	if _, _, err := projectEnvironmentComponentService(environment, GeneratedEnvironmentService{ComponentID: componentID, Name: testComponentServiceName, Definition: definition}, image); err == nil {
		t.Fatal("projection accepted invalid healthcheck")
	}
}

const (
	testComponentServiceName                = "cloudflare-tunnel"
	testComponentServiceImageRepository     = "docker.io/cloudflare/cloudflared"
	testComponentServiceImageIndexDigest    = "4f6655284ab3d252b7f28fedb19fe6c8fc82ee5b1295c20ac74d475e5398a52d"
	testComponentServiceAMD64ManifestDigest = "18626b1baac4450214535cd5bc40ef44c0635244d585ebf707749c22b6f3408f"
	testComponentServiceAMD64ConfigDigest   = "e871921d7924ab4baa36da9938ecddb86025b5b1aa930500769456bb24f50a75"
	testComponentServiceARM64ManifestDigest = "a85d5a3d6f22cb3c7e78b2f0d05b0f0daeb72566e9426f656c60b357b7b89c95"
	testComponentServiceARM64ConfigDigest   = "5d249c08c07ddc00eb501917e030874347a8ea786bdb644bb2cff92a4cd2b843"
)

func componentServiceTestImage() component.OCIImage {
	return component.OCIImage{
		Repository:  testComponentServiceImageRepository,
		IndexDigest: testComponentServiceImageIndexDigest,
		Platforms: []component.OCIPlatform{
			{
				OS: "linux", Architecture: "amd64",
				ChildDigest:  testComponentServiceAMD64ManifestDigest,
				ConfigDigest: testComponentServiceAMD64ConfigDigest,
			},
			{
				OS: "linux", Architecture: "arm64",
				ChildDigest:  testComponentServiceARM64ManifestDigest,
				ConfigDigest: testComponentServiceARM64ConfigDigest,
			},
		},
	}
}
