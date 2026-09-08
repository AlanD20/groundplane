package controller

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentRenderTestRenderer struct {
	serviceName string
	filePath    string
}

func (renderer componentRenderTestRenderer) Plan(
	_ core.Environment,
	component core.Component,
) (componentsdk.EnvironmentPlan, error) {
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{
			{
				ID:          component.GeneratedServices[0],
				Name:        renderer.serviceName,
				Image:       controllerTestOCIImage("example/component"),
				NetworkMode: componentsdk.ManagedNetworkModeZones,
				Networks:    []componentsdk.ManagedNetworkAttachment{{Name: "frontend"}},
				Restart:     "unless-stopped",
				Replicas:    1,
			},
		},
		Files: []componentsdk.ManagedFile{{Path: renderer.filePath, Content: []byte(renderer.serviceName)}},
	}, nil
}

// Rationale: every registered renderer must fold into one deterministic,
// owner-tagged result regardless of catalog or durable Component order.
func TestRenderEnvironmentComponentsCompilesDeterministicCatalog(t *testing.T) {
	environment := componentRenderTestEnvironment()
	catalog := []EnvironmentComponentRegistration{
		componentRenderTestRegistration(
			t,
			core.ComponentKindEdgeCloudflare,
			componentRenderTestRenderer{serviceName: "tunnel", filePath: "components/tunnel/config"},
		),
		componentRenderTestRegistration(
			t,
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
	catalog := []EnvironmentComponentRegistration{componentRenderTestRegistration(
		t,
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
				Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
					ZoneIDs:  []string{ids.NewAt(ids.KindNetwork, at, 901)},
					SecretID: ids.NewAt(ids.KindSecret, at, 907),
				}},
			},
			{
				ID: ids.NewAt(ids.KindComponent, at, 904), Owner: core.ComponentOwnerEnvironment,
				OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
				GeneratedServices: []string{ids.NewAt(ids.KindService, at, 906)},
				Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
					ZoneIDs: []string{ids.NewAt(ids.KindNetwork, at, 901)},
				}},
			},
		},
	}
}

func componentRenderTestTime() time.Time {
	return time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
}

func componentRenderTestRegistration(
	t *testing.T,
	kind core.ComponentKind,
	renderer componentRenderTestRenderer,
) EnvironmentComponentRegistration {
	return componentTestRegistration(t, kind, renderer.Plan)
}

func componentTestRegistration(
	t *testing.T,
	kind core.ComponentKind,
	plan EnvironmentComponentPlanFunc,
) EnvironmentComponentRegistration {
	t.Helper()
	action, err := componentsdk.NewActionDefinition(
		componentsdk.ActionID("activate-config"),
		componentsdk.CapabilityManagedConfig,
		componentsdk.OperationActivate,
	)
	if err != nil {
		t.Fatalf("NewActionDefinition() error = %v", err)
	}
	definition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: componentsdk.ImplementationKey(kind),
		ConfigVariant:  componentsdk.ConfigVariant("test"),
		Provides:       []componentsdk.Capability{componentsdk.CapabilityServices},
		OwnerScopes:    []componentsdk.OwnerScope{componentsdk.OwnerScopeEnvironment},
		Actions:        []componentsdk.ActionDefinition{action},
	})
	if err != nil {
		t.Fatalf("NewDefinition() error = %v", err)
	}
	return EnvironmentComponentRegistration{
		Kind: kind, Definition: definition,
		CatalogDigest: sha256.Sum256([]byte("test Component catalog:" + string(kind))),
		Plan:          plan,
	}
}
