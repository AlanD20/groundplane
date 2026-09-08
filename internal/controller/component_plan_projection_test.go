package controller

import (
	"context"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type componentPlanProjectionRenderer struct {
	serviceName string
}

func (renderer componentPlanProjectionRenderer) Plan(
	environment core.Environment,
	component core.Component,
) (componentsdk.EnvironmentPlan, error) {
	if !component.Enabled {
		return componentsdk.EnvironmentPlan{}, nil
	}
	services := []componentsdk.ManagedService{{
		ID: component.GeneratedServices[0], Name: renderer.serviceName, Image: controllerTestOCIImage("example/router"),
		NetworkMode: componentsdk.ManagedNetworkModeZones,
		Networks:    []componentsdk.ManagedNetworkAttachment{{Name: "frontend", StaticIPv4: component.PinnedIPv4}},
		Expose:      []string{"80"}, Restart: "unless-stopped", Replicas: 1,
		Mounts: []componentsdk.ManagedMount{{
			Source: "components/router/config", Target: "/etc/router/config", ReadOnly: true,
		}},
	}}
	routeID := "none"
	if len(environment.Routes) != 0 {
		routeID = environment.Routes[0].ID
	}
	content := []byte(routeID + "\n")
	if service, exists := environment.Services["api"]; exists && service.Strategy != "" {
		dependency := service.DependsOn["migrate"]
		content = []byte(
			routeID + "\nstrategy=" + string(service.Strategy) +
				"\ndependency=" + dependency.Condition.String() + ":" + dependency.Phases[0].String() + "\n",
		)
	}
	return componentsdk.EnvironmentPlan{
		Services: services,
		Files:    []componentsdk.ManagedFile{{Path: "components/router/config", Content: content}},
	}, nil
}

// Rationale: restart-time Component rendering must reuse the exact stable
// Route, Component, and generated Service identities pinned at task creation.
func TestProjectPinnedEnvironmentComponentsRehydratesExactGraph(t *testing.T) {
	project, identity, projection, routeSpecs, catalog := componentPlanProjectionInput(t)
	project.Services["migrate"] = composetypes.ServiceConfig{
		Name: "migrate", Image: "migrate:1",
		Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
	}
	appendMigrationIdentity(&projection)

	result, err := projectPinnedEnvironmentComponents(
		project,
		map[string]core.ServiceExtensionSpec{"api": {
			Release: &core.ServiceReleaseSpec{DefaultStrategy: core.StrategyBlueGreen},
			DependsOn: map[string]core.ServiceDependency{"migrate": {
				Condition: core.ServiceDependencyCompletedSuccessfully,
				Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy},
			}},
		}},
		identity,
		projection,
		routeSpecs,
		map[string]core.ComponentSpec{string(core.ComponentCapabilityHTTPRouter): {
			Implementation: core.ComponentKindIngressCaddy, Enabled: true,
			Settings: core.ComponentCapabilitySettings{ZoneIDs: []string{projection.DesiredZones[0].Desired.ID}},
		}},
		nil,
		catalog,
	)
	if err != nil {
		t.Fatalf("projectPinnedEnvironmentComponents() error = %v", err)
	}
	generated, exists := result.Project.Services["caddy"]
	if !exists || generated.Networks["frontend"].Ipv4Address != "10.70.0.2" ||
		len(result.PlainFiles) != 1 || result.PlainFiles[0].ComponentID != projection.Components[0].Desired.ID ||
		string(result.PlainFiles[0].Content) != projection.DesiredRoutes[0].Desired.ID+
			"\nstrategy=blue-green\ndependency=service_completed_successfully:deploy\n" {
		t.Fatalf("Component projection = %#v", result)
	}
}

// Rationale: the desired Route records are the sole replay authority; a
// removed Route must not be reconstructed from the retained Blueprint extension.
func TestProjectPinnedEnvironmentComponentsUsesDesiredRoutesDirectly(t *testing.T) {
	project, identity, projection, routeSpecs, catalog := componentPlanProjectionInput(t)
	projection.DesiredRoutes = nil

	result, err := projectPinnedEnvironmentComponents(
		project,
		nil,
		identity,
		projection,
		routeSpecs,
		map[string]core.ComponentSpec{string(core.ComponentCapabilityHTTPRouter): {
			Implementation: core.ComponentKindIngressCaddy, Enabled: true,
			Settings: core.ComponentCapabilitySettings{ZoneIDs: []string{projection.DesiredZones[0].Desired.ID}},
		}},
		nil,
		catalog,
	)
	if err != nil {
		t.Fatalf("projectPinnedEnvironmentComponents() error = %v", err)
	}
	if len(result.PlainFiles) != 1 || string(result.PlainFiles[0].Content) != "none\n" {
		t.Fatalf("removed Route Component projection = %#v", result)
	}
}

// Rationale: an explicit Route removal candidate must retain enough immutable
// match history to replay the Blueprint while omitting that Route from the
// effective Component renderer input.
func TestProjectPinnedEnvironmentComponentsOmitsSuppressedRoute(t *testing.T) {
	project, identity, projection, routeSpecs, catalog := componentPlanProjectionInput(t)
	next := projection
	next.DesiredRoutes = nil
	result, err := projectPinnedEnvironmentComponents(
		project,
		nil,
		identity,
		next,
		routeSpecs,
		map[string]core.ComponentSpec{string(core.ComponentCapabilityHTTPRouter): {
			Implementation: core.ComponentKindIngressCaddy, Enabled: true,
			Settings: core.ComponentCapabilitySettings{ZoneIDs: []string{projection.DesiredZones[0].Desired.ID}},
		}},
		nil,
		catalog,
	)
	if err != nil {
		t.Fatalf("projectPinnedEnvironmentComponents() error = %v", err)
	}
	if len(result.PlainFiles) != 1 || string(result.PlainFiles[0].Content) != "none\n" {
		t.Fatalf("suppressed Component projection = %#v", result)
	}
}

// Rationale: a Controller restart must regenerate the exact Component file
// from retained Blueprint and projection state without stored generated bytes.
func TestTaskPlanResolverRegeneratesPinnedComponentFile(t *testing.T) {
	_, identity, projection, _, catalog := componentPlanProjectionInput(t)
	appendMigrationIdentity(&projection)
	identity.TenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	identity.TenantSlug = "acme"
	identity.ProjectSlug = "shop"
	content := []byte(`kind: environment
schema: 1
metadata: {tenant: acme, project: shop, environment: production}
x-gp-network-pool: 10.70.0.0/16
services:
  migrate:
    image: migrate:1
    networks: [frontend]
  api:
    image: api:1
    x-gp-release: {default_strategy: blue-green}
    x-gp-depends_on:
      migrate: {condition: service_completed_successfully, phases: [deploy]}
    networks: [frontend]
    expose: ["8080"]
networks:
  frontend:
    ipam:
      config:
        - subnet: 10.70.0.0/24
x-gp-routes:
  - hostname: app.example.com
    path: /app/*
    target: api
    target_port: 8080
    exposure: public
x-gp-components:
  http-router:
    implementation: caddy
    enabled: true
    settings:
      zone_ids: [` + projection.DesiredZones[0].Desired.ID + "]\n")
	parsed, err := blueprintparser.Parse(t.Context(), blueprintparser.EnvironmentScope{
		EnvironmentID: identity.EnvironmentID, Tenant: identity.TenantSlug,
		Project: identity.ProjectSlug, Environment: identity.EnvironmentName,
	}, core.BlueprintBundle{RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files: []core.BlueprintFile{{Path: "blueprint.yaml", Content: content}}})
	if err != nil {
		t.Fatal(err)
	}
	// An ordinary Zone omitted from this upload remains in the pinned desired
	// revision and must not make Component file replay depend on audit coverage.
	parsed.Project.Networks["secondary"] = composetypes.NetworkConfig{
		Ipam: composetypes.IPAMConfig{Config: []*composetypes.IPAMPool{{Subnet: "10.70.1.0/24"}}},
	}
	projection.DesiredZones = append(projection.DesiredZones, etcd.EnvironmentZoneProjection{
		EnvironmentID: identity.EnvironmentID, Desired: core.Zone{
			ID:   ids.NewAt(ids.KindNetwork, time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC), 30),
			Name: "secondary", Subnet: "10.70.1.0/24",
			OwnerKind: core.ZoneOwnerEnvironment, OwnerID: identity.EnvironmentID,
		},
	})
	projection.NormalizedCompose, err = MarshalNormalizedEnvironmentProject(parsed.Project)
	if err != nil {
		t.Fatal(err)
	}
	reader := &blueprintPlanReader{
		tenant: etcd.TenantRecord{ID: identity.TenantID, Slug: identity.TenantSlug, Name: "Acme"},
		project: etcd.ProjectRecord{
			ID: identity.ProjectID, TenantID: identity.TenantID, Slug: identity.ProjectSlug,
			Name: "Shop", Kind: etcd.ProjectKindTenant,
		},
		environment: etcd.EnvironmentRecord{
			ID: identity.EnvironmentID, ProjectID: identity.ProjectID, Name: identity.EnvironmentName,
			NetworkPool: "10.70.0.0/16", VolumeDir: identity.AuthorizedVolumeDir,
			ProvisioningState: etcd.EnvironmentProvisioningReady,
		},
		revision: etcd.EnvironmentBlueprintRevision{
			EnvironmentID: identity.EnvironmentID, RevisionID: projection.RevisionID,
			RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
			Files:     []etcd.EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: content}},
			CreatedAt: time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
		},
		projection: projection,
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, catalog)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	actual, err := resolver.ResolveComponentFile(context.Background(), identity.EnvironmentID,
		etcd.TaskComponentFileValueReference{
			RevisionID:  projection.RevisionID,
			ComponentID: projection.Components[0].Desired.ID,
			Path:        "components/router/config",
		})
	if err != nil {
		t.Fatalf("ResolveComponentFile() error = %v", err)
	}
	if string(actual) != projection.DesiredRoutes[0].Desired.ID+
		"\nstrategy=blue-green\ndependency=service_completed_successfully:deploy\n" {
		t.Fatalf("ResolveComponentFile() = %q", actual)
	}
}

func componentPlanProjectionInput(
	t *testing.T,
) (*composetypes.Project, pinnedEnvironmentIdentity, etcd.EnvironmentComposeProjection, []core.RouteSpec, []EnvironmentComponentRegistration) {
	t.Helper()
	at := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	apiID := ids.NewAt(ids.KindService, at, 3)
	caddyServiceID := ids.NewAt(ids.KindService, at, 4)
	caddy, err := etcd.NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 5), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		GeneratedServices: []string{caddyServiceID}, PinnedIPv4: "10.70.0.2",
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, at, 9)},
		}},
	})
	if err != nil {
		t.Fatalf("etcd.NewComponentRecord(caddy) error = %v", err)
	}
	tunnel, err := etcd.NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 6), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("etcd.NewComponentRecord(tunnel) error = %v", err)
	}
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {
			Name: "api", Image: "api:1", Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
			Expose: composetypes.StringOrNumberList{"8080"},
		}},
		Networks: composetypes.Networks{"frontend": {
			Name: "frontend",
			Ipam: composetypes.IPAMConfig{Config: []*composetypes.IPAMPool{{Subnet: "10.70.0.0/24"}}},
		}},
	}
	identity := pinnedEnvironmentIdentity{
		ProjectID: projectID, EnvironmentID: environmentID, EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + ids.NewAt(ids.KindTenant, at, 7) + "/" +
			projectID + "/" + environmentID,
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, at, 8), RenderGeneration: 1,
		DesiredServices: []etcd.EnvironmentServiceProjection{{EnvironmentID: environmentID, Desired: core.Service{
			ID: apiID, Name: "api", Image: "api:1", Zones: []string{"frontend"},
		}}},
		DesiredZones: []etcd.EnvironmentZoneProjection{{EnvironmentID: environmentID, Desired: core.Zone{
			ID: ids.NewAt(ids.KindNetwork, at, 9), Name: "frontend", Subnet: "10.70.0.0/24",
			OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		}}},
		DesiredRoutes: []etcd.EnvironmentRouteProjection{{EnvironmentID: environmentID, Desired: core.Route{
			ID: ids.NewAt(ids.KindRoute, at, 10), Host: "app.example.com", Path: "/app/*",
			TargetServiceID: apiID, TargetPort: 8080, Exposure: "public",
		}}},
		Components: []etcd.ComponentRecord{caddy, tunnel},
	}
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatalf("marshal normalized Component projection: %v", err)
	}
	projection.NormalizedCompose = normalized
	projection.ComposeArtifact = normalizedProjectionArtifactFixture(
		t,
		ids.NewAt(ids.KindConfig, at, 11),
		environmentID,
		identity.AuthorizedVolumeDir,
		normalized,
		[]*agentpb.ComposeService{
			{ServiceId: apiID, ComposeName: "api"},
			{ServiceId: caddyServiceID, ComposeName: "caddy"},
		},
		nil,
	)
	routeSpecs := []core.RouteSpec{{
		Hostname: "app.example.com", Path: "/app/*", Target: "api", TargetPort: 8080, Exposure: "public",
	}}
	catalog := []EnvironmentComponentRegistration{
		componentTestRegistration(
			t, core.ComponentKindIngressCaddy,
			componentPlanProjectionRenderer{serviceName: "caddy"}.Plan,
		),
		componentTestRegistration(
			t, core.ComponentKindEdgeCloudflare,
			componentPlanProjectionRenderer{serviceName: "cloudflare-tunnel"}.Plan,
		),
	}
	return project, identity, projection, routeSpecs, catalog
}

func appendMigrationIdentity(projection *etcd.EnvironmentComposeProjection) {
	at := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)
	migrationID := ids.NewAt(ids.KindService, at, 11)
	projection.DesiredServices = append(projection.DesiredServices, etcd.EnvironmentServiceProjection{
		EnvironmentID: projection.EnvironmentID,
		Desired:       core.Service{ID: migrationID, Name: "migrate", Image: "migrate:1", Zones: []string{"frontend"}},
	})
}
