package composerender

import (
	"encoding/hex"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomponentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: selected registry child and local image/config identity are distinct
// and must both survive Component projection into the wire and Compose labels.
func TestRenderComponentComposeSealsSelectedImageIdentity(t *testing.T) {
	for _, repository := range []string{"docker.io/library/caddy", "docker.io/cloudflare/cloudflared"} {
		t.Run(repository, func(t *testing.T) {
			image := controllerTestOCIImage(repository)
			platform, reference, selected := image.Select(runtime.GOOS, runtime.GOARCH)
			if !selected {
				t.Fatal("fixture lacks test platform")
			}
			project := &composetypes.Project{Services: composetypes.Services{"managed": {Image: reference}}}
			input := composeRenderTestInput(project)
			selection := composeidentity.ComponentImage{
				Repository:  repository,
				IndexDigest: image.IndexDigest,
				Reference:   reference,
				Platform:    platform,
			}
			input.Identities.Services = []composeidentity.Resource{{
				ID: composeIdentityTestID(ids.KindService, 70), Name: "managed",
				ComponentID: composeIdentityTestID(ids.KindComponent, 71), ComponentImage: &selection,
			}}
			artifact, err := RenderCompose(input)
			if err != nil {
				t.Fatal(err)
			}
			service := artifact.Services[0]
			if service.GetImageReference() != reference || service.GetImageRepository() != repository ||
				hex.EncodeToString(service.GetImageIndexDigest()) != image.IndexDigest ||
				hex.EncodeToString(service.GetImageChildDigest()) != platform.ChildDigest ||
				hex.EncodeToString(service.GetImageConfigDigest()) != platform.ConfigDigest ||
				service.GetImageOs() != platform.OS || service.GetImageArchitecture() != platform.Architecture ||
				service.GetImageVariant() != platform.Variant {
				t.Fatalf("selected Component image authority changed: %v", service)
			}
			if !strings.Contains(string(artifact.GetCanonicalYaml()), "sha256:"+platform.ConfigDigest) {
				t.Fatal("canonical Compose omitted selected config identity label")
			}
			input.Identities.Services[0].ComponentImage = nil
			if _, err := RenderCompose(input); err == nil {
				t.Fatal("render accepted missing Component image authority")
			}
		})
	}
}

type componentComposeTestRenderer struct {
	unsupported bool
}

func (renderer componentComposeTestRenderer) Plan(
	environment core.Environment,
	component core.Component,
) (componentsdk.EnvironmentPlan, error) {
	service := componentsdk.ManagedService{
		ID: component.GeneratedServices[0], Name: "router", Image: controllerTestOCIImage("example/router"),
		NetworkMode: componentsdk.ManagedNetworkModeZones,
		Networks: []componentsdk.ManagedNetworkAttachment{{
			Name: "frontend", Aliases: []string{"router"}, StaticIPv4: "10.60.0.2", GatewayPriority: 1,
		}},
		Dependencies: []componentsdk.ManagedDependency{{ServiceName: "app", Condition: "service_started"}},
		Command:      []string{"serve"}, Expose: []string{"443"}, Restart: "unless-stopped", Replicas: 1,
		Mounts: []componentsdk.ManagedMount{
			{Source: "components/router/config", Target: "/etc/router/config", ReadOnly: true},
			{Source: "components/router/data", Target: "/var/lib/router"},
		},
		SecretEnvironment: []componentsdk.ManagedSecretEnvironment{{
			Name: "TOKEN", SecretID: ids.NewAt(ids.KindSecret, componentRenderTestTime(), 20),
		}},
	}
	if renderer.unsupported {
		service.Image = componentsdk.OCIImage{}
	}
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{service},
		Files: []componentsdk.ManagedFile{
			{Path: "components/router/config", Content: []byte("routes\n")},
			{Path: "components/router/data/.groundplane-managed", Content: []byte{}},
		},
	}, nil
}

// Rationale: Environment components must enter the authored Compose project
// with stable identities and host paths while secret bytes remain exclusively
// in the transient materialization pipeline.
func TestProjectEnvironmentComponentsBuildsComposeAndMaterializationInputs(t *testing.T) {
	environment, catalog, project := componentComposeTestInput(t, false)

	projection, err := ProjectEnvironmentComponents(project, environment, catalog)
	if err != nil {
		t.Fatalf("ProjectEnvironmentComponents() error = %v", err)
	}
	if len(project.Services) != 1 {
		t.Fatalf("input project Services = %#v, want unchanged", project.Services)
	}
	generated, exists := projection.Project.Services["router"]
	if !exists ||
		generated.Image != controllerTestOCIImageReference("example/router") ||
		len(generated.Command) != 1 || generated.Command[0] != "serve" ||
		generated.Restart != "unless-stopped" || generated.Deploy == nil ||
		generated.Deploy.Replicas == nil ||
		*generated.Deploy.Replicas != 1 {
		t.Fatalf("generated Compose Service = %#v", generated)
	}
	network := generated.Networks["frontend"]
	if network == nil || network.Ipv4Address != "10.60.0.2" || network.GatewayPriority != 1 ||
		len(network.Aliases) != 1 ||
		network.Aliases[0] != "router" {
		t.Fatalf("generated network attachment = %#v", network)
	}
	if len(generated.Volumes) != 2 || generated.Volumes[0].Source !=
		filepath.Join(environment.VolumeDir, "components/router/config") ||
		generated.Volumes[0].Type != composetypes.VolumeTypeBind || !generated.Volumes[0].ReadOnly ||
		generated.Volumes[0].Bind == nil || !bool(generated.Volumes[0].Bind.CreateHostPath) {
		t.Fatalf("generated mounts = %#v", generated.Volumes)
	}
	if dependency, exists := generated.DependsOn["app"]; !exists ||
		dependency.Condition != "service_started" || !dependency.Required {
		t.Fatalf("generated dependency = %#v", generated.DependsOn)
	}
	if len(projection.Services) != 1 || projection.Services[0].ID != environment.Components[0].GeneratedServices[0] ||
		projection.Services[0].Name != "router" || projection.Services[0].ComponentImage == nil ||
		*projection.Services[0].ComponentImage != testSelectedComponentImage("example/router") {
		t.Fatalf("generated identities = %#v", projection.Services)
	}
	if len(projection.PlainFiles) != 2 || projection.PlainFiles[0].Path != "components/router/config" ||
		string(projection.PlainFiles[0].Content) != "routes\n" ||
		projection.PlainFiles[1].Path != "components/router/data/.groundplane-managed" {
		t.Fatalf("plain files = %#v", projection.PlainFiles)
	}
	if len(projection.EnvironmentFiles) != 1 {
		t.Fatalf("Environment files = %#v", projection.EnvironmentFiles)
	}
	environmentFile := projection.EnvironmentFiles[0]
	wantDestination := "secrets/.env." + environment.ID + ".router"
	if environmentFile.ServiceID != environment.Components[0].GeneratedServices[0] ||
		environmentFile.ServiceName != "router" || environmentFile.Destination != wantDestination ||
		len(environmentFile.Values) != 1 || environmentFile.Values[0].Name != "TOKEN" ||
		len(
			generated.EnvFiles,
		) != 1 || generated.EnvFiles[0].Path != filepath.Join(environment.VolumeDir, wantDestination) {
		t.Fatalf("generated Environment file = %#v; Compose env files = %#v", environmentFile, generated.EnvFiles)
	}
}

// Rationale: registered implementations are untrusted plan producers, so an
// invalid SDK service must fail before it reaches Compose projection.
func TestProjectEnvironmentComponentsRejectsInvalidManagedService(t *testing.T) {
	environment, catalog, project := componentComposeTestInput(t, true)

	_, err := ProjectEnvironmentComponents(project, environment, catalog)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ProjectEnvironmentComponents() error = %v, want internal", err)
	}
}

// Rationale: a generated service cannot safely join a Zone that is durable in
// the Environment projection but absent from the exact parsed Compose input.
func TestProjectEnvironmentComponentsRejectsMissingComposeZone(t *testing.T) {
	environment, catalog, project := componentComposeTestInput(t, false)
	project.Networks = nil

	_, err := ProjectEnvironmentComponents(project, environment, catalog)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ProjectEnvironmentComponents() error = %v, want internal", err)
	}
}

func componentComposeTestInput(
	t *testing.T,
	unsupported bool,
) (core.Environment, []testcomponentrender.EnvironmentComponentRegistration, *composetypes.Project) {
	at := time.Date(2026, 8, 22, 21, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 950)
	componentID := ids.NewAt(ids.KindComponent, at, 951)
	serviceID := ids.NewAt(ids.KindService, at, 952)
	entryID := ids.NewAt(ids.KindEnvEntry, at, 953)
	environment := core.Environment{
		ID: environmentID,
		VolumeDir: "/var/lib/groundplane/vol/" + ids.NewAt(ids.KindTenant, at, 954) + "/" +
			ids.NewAt(ids.KindProject, at, 955) + "/" + environmentID,
		Zones: map[string]core.Zone{"frontend": {
			ID: ids.NewAt(ids.KindNetwork, at, 956), Name: "frontend", Subnet: "10.60.0.0/24",
		}},
		Services: map[string]core.Service{"app": {
			ID: ids.NewAt(ids.KindService, at, 957), Name: "app", Image: "app:1", Zones: []string{"frontend"},
		}},
		Components: []core.Component{{
			ID: componentID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
			Kind: core.ComponentKindIngressCaddy, Enabled: true, GeneratedServices: []string{serviceID},
			Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
				ZoneIDs: []string{ids.NewAt(ids.KindNetwork, at, 956)},
			}},
		}},
		Entries: []core.EnvEntry{{
			ID: entryID, Kind: core.EntryKindEnv, Key: "ROUTER_TOKEN",
			Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"router"}, Secret: true,
		}},
	}
	implementation := componentComposeTestRenderer{unsupported: unsupported}
	catalog := []testcomponentrender.EnvironmentComponentRegistration{componentTestRegistration(
		t, core.ComponentKindIngressCaddy, implementation.Plan,
	)}
	project := &composetypes.Project{
		Services: composetypes.Services{"app": {Name: "app", Image: "app:1"}},
		Networks: composetypes.Networks{"frontend": {Name: "frontend"}},
	}
	return environment, catalog, project
}
