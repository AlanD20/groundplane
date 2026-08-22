package controller

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type componentComposeTestRenderer struct {
	unsupported bool
}

func (renderer componentComposeTestRenderer) Render(
	environment core.Environment,
	component core.Component,
) (map[string]components.GeneratedService, map[string][]byte, error) {
	service := core.Service{
		ID: component.GeneratedServices[0], Name: "router", Image: "router:1",
		Zones: []string{"frontend"}, Command: []string{"serve"},
		Aliases:   map[string][]string{"frontend": {"router"}},
		DependsOn: map[string]core.ServiceDependency{"app": {Condition: "service_started"}},
		Expose:    []string{"443"}, Restart: "unless-stopped", Replicas: 1,
	}
	if renderer.unsupported {
		service.Resources.Mem = "64m"
	}
	return map[string]components.GeneratedService{
		"router": {
			Service:    service,
			StaticIPv4: map[string]string{"frontend": "10.60.0.2"},
			Mounts: []components.GeneratedMount{
				{Source: "components/router/config", Target: "/etc/router/config", ReadOnly: true},
				{Source: "components/router/data", Target: "/var/lib/router"},
			},
			SecretEnvironment: []components.GeneratedSecretEnvironment{{
				Name: "TOKEN", EntryID: environment.Entries[0].ID,
			}},
		},
	}, map[string][]byte{"components/router/config": []byte("routes\n")}, nil
}

func (componentComposeTestRenderer) Healthy(core.Environment, core.Component) (bool, error) {
	return true, nil
}

// Rationale: Environment components must enter the authored Compose project
// with stable identities and host paths while secret bytes remain exclusively
// in the transient materialization pipeline.
func TestProjectEnvironmentComponentsBuildsComposeAndMaterializationInputs(t *testing.T) {
	environment, catalog, project := componentComposeTestInput(false)

	projection, err := ProjectEnvironmentComponents(project, environment, catalog)
	if err != nil {
		t.Fatalf("ProjectEnvironmentComponents() error = %v", err)
	}
	if len(project.Services) != 1 {
		t.Fatalf("input project Services = %#v, want unchanged", project.Services)
	}
	generated, exists := projection.Project.Services["router"]
	if !exists || generated.Image != "router:1" || len(generated.Command) != 1 || generated.Command[0] != "serve" ||
		generated.Restart != "unless-stopped" || generated.Deploy == nil || generated.Deploy.Replicas == nil ||
		*generated.Deploy.Replicas != 1 {
		t.Fatalf("generated Compose Service = %#v", generated)
	}
	network := generated.Networks["frontend"]
	if network == nil || network.Ipv4Address != "10.60.0.2" || len(network.Aliases) != 1 ||
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
		projection.Services[0].Name != "router" {
		t.Fatalf("generated identities = %#v", projection.Services)
	}
	if len(projection.PlainFiles) != 1 || projection.PlainFiles[0].Path != "components/router/config" ||
		string(projection.PlainFiles[0].Content) != "routes\n" {
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

// Rationale: registry implementations are extensible, so a renderer using an
// unimplemented field must fail rather than produce a lossy Compose service.
func TestProjectEnvironmentComponentsRejectsUnsupportedGeneratedServiceField(t *testing.T) {
	environment, catalog, project := componentComposeTestInput(true)

	_, err := ProjectEnvironmentComponents(project, environment, catalog)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ProjectEnvironmentComponents() error = %v, want internal", err)
	}
}

// Rationale: a generated service cannot safely join a Zone that is durable in
// the Environment projection but absent from the exact parsed Compose input.
func TestProjectEnvironmentComponentsRejectsMissingComposeZone(t *testing.T) {
	environment, catalog, project := componentComposeTestInput(false)
	project.Networks = nil

	_, err := ProjectEnvironmentComponents(project, environment, catalog)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ProjectEnvironmentComponents() error = %v, want internal", err)
	}
}

func componentComposeTestInput(
	unsupported bool,
) (core.Environment, []components.Registration, *composetypes.Project) {
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
		}},
		Entries: []core.EnvEntry{{
			ID: entryID, Kind: core.EntryKindEnv, Key: "ROUTER_TOKEN",
			Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"router"}, Secret: true,
		}},
	}
	implementation := componentComposeTestRenderer{unsupported: unsupported}
	catalog := []components.Registration{{
		Kind: core.ComponentKindIngressCaddy, Label: "test",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerEnvironment},
		ApplyStrategy: components.EnvironmentRender, Environment: implementation,
	}}
	project := &composetypes.Project{
		Services: composetypes.Services{"app": {Name: "app", Image: "app:1"}},
		Networks: composetypes.Networks{"frontend": {Name: "frontend"}},
	}
	return environment, catalog, project
}
