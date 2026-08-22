package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentRenderTestRenderer struct {
	serviceName string
	filePath    string
}

func (renderer componentRenderTestRenderer) Render(
	_ core.Environment,
	component core.Component,
) (map[string]components.GeneratedService, map[string][]byte, error) {
	return map[string]components.GeneratedService{
		renderer.serviceName: {
			Service: core.Service{
				ID: component.GeneratedServices[0], Name: renderer.serviceName, Image: "example/component:1",
				Zones: []string{"frontend"}, Restart: "unless-stopped", Replicas: 1,
			},
		},
	}, map[string][]byte{renderer.filePath: []byte(renderer.serviceName)}, nil
}

func (renderer componentRenderTestRenderer) Healthy(core.Environment, core.Component) (bool, error) {
	return true, nil
}

// Rationale: every registered renderer must fold into one deterministic,
// owner-tagged result regardless of catalog or durable Component order.
func TestRenderEnvironmentComponentsCompilesDeterministicCatalog(t *testing.T) {
	environment := componentRenderTestEnvironment()
	catalog := []components.Registration{
		componentRenderTestRegistration(
			core.ComponentKindEdgeCloudflare,
			componentRenderTestRenderer{serviceName: "tunnel", filePath: "components/tunnel/config"},
		),
		componentRenderTestRegistration(
			core.ComponentKindIngressCaddy,
			componentRenderTestRenderer{serviceName: "caddy", filePath: "components/caddy/Caddyfile"},
		),
	}

	rendered, err := RenderEnvironmentComponents(environment, catalog)
	if err != nil {
		t.Fatalf("RenderEnvironmentComponents() error = %v", err)
	}
	if len(rendered.Services) != 2 || rendered.Services[0].Name != "caddy" ||
		rendered.Services[1].Name != "tunnel" || rendered.Services[0].ComponentID != environment.Components[1].ID {
		t.Fatalf("rendered Services = %#v", rendered.Services)
	}
	if len(rendered.Files) != 2 || rendered.Files[0].Path != "components/caddy/Caddyfile" ||
		rendered.Files[1].Path != "components/tunnel/config" {
		t.Fatalf("rendered Files = %#v", rendered.Files)
	}
}

// Rationale: generated Services share one Compose namespace with authored
// Services and must fail before dispatch when a component claims an existing
// name.
func TestRenderEnvironmentComponentsRejectsAuthoredServiceCollision(t *testing.T) {
	environment := componentRenderTestEnvironment()
	environment.Services = map[string]core.Service{
		"caddy": {
			ID: ids.NewAt(ids.KindService, componentRenderTestTime(), 905), Name: "caddy", Image: "authored:1",
		},
	}
	catalog := []components.Registration{componentRenderTestRegistration(
		core.ComponentKindIngressCaddy,
		componentRenderTestRenderer{serviceName: "caddy", filePath: "components/caddy/Caddyfile"},
	)}
	environment.Components = environment.Components[1:]

	_, err := RenderEnvironmentComponents(environment, catalog)
	if !errors.Is(err, errs.New(errs.KindNameConflict, "")) {
		t.Fatalf("RenderEnvironmentComponents() error = %v, want name.conflict", err)
	}
}

func componentRenderTestEnvironment() core.Environment {
	at := componentRenderTestTime()
	environmentID := ids.NewAt(ids.KindEnvironment, at, 900)
	return core.Environment{
		ID: environmentID,
		Zones: map[string]core.Zone{
			"frontend": {
				ID: ids.NewAt(ids.KindNetwork, at, 901), Name: "frontend", Subnet: "10.40.0.0/24",
			},
		},
		Components: []core.Component{
			{
				ID: ids.NewAt(ids.KindComponent, at, 902), Owner: core.ComponentOwnerEnvironment,
				OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
				GeneratedServices: []string{ids.NewAt(ids.KindService, at, 903)},
			},
			{
				ID: ids.NewAt(ids.KindComponent, at, 904), Owner: core.ComponentOwnerEnvironment,
				OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
				GeneratedServices: []string{ids.NewAt(ids.KindService, at, 906)},
			},
		},
	}
}

func componentRenderTestTime() time.Time {
	return time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
}

func componentRenderTestRegistration(
	kind core.ComponentKind,
	renderer componentRenderTestRenderer,
) components.Registration {
	return components.Registration{
		Kind: kind, Label: string(kind), AllowedOwners: []core.ComponentOwner{core.ComponentOwnerEnvironment},
		ApplyStrategy: components.EnvironmentRender, Environment: renderer,
	}
}
