package taskplanning

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	sdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	compose "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type nativeRouteFixture struct {
	project     hierarchy.ProjectRecord
	environment hierarchy.EnvironmentRecord
	projection  environmentprojection.EnvironmentComposeProjection
	catalog     []componentrender.EnvironmentComponentRegistration
	state       *nativeRouteState
}

func newNativeRouteFixture(t *testing.T) nativeRouteFixture {
	t.Helper()
	environment, tenant, project := ids.New(ids.KindEnvironment), ids.New(ids.KindTenant), ids.New(ids.KindProject)
	service, router, component, zone := ids.New(
		ids.KindService,
	), ids.New(
		ids.KindService,
	), ids.New(
		ids.KindComponent,
	), ids.New(
		ids.KindNetwork,
	)
	owned, err := components.NewRecord(core.Component{ID: component, Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		GeneratedServices: []string{router}, PinnedIPv4: "10.70.0.2",
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{zone}}}})
	if err != nil {
		t.Fatal(err)
	}
	tunnel, err := components.NewRecord(
		core.Component{ID: ids.New(ids.KindComponent), Owner: core.ComponentOwnerEnvironment,
			OwnerID: environment, Kind: core.ComponentKindEdgeCloudflare},
	)
	if err != nil {
		t.Fatal(err)
	}
	projected := environmentprojection.EnvironmentComposeProjection{EnvironmentID: environment,
		RevisionID: ids.New(ids.KindTask), RenderGeneration: 1, Components: []components.Record{owned, tunnel},
		DesiredServices: []services.EnvironmentServiceProjection{{EnvironmentID: environment,
			Desired: core.Service{ID: service, Name: "api", Image: "app:captured", Zones: []string{"frontend"}}}},
		DesiredZones: []environmentprojection.EnvironmentZoneProjection{{EnvironmentID: environment,
			Desired: core.Zone{
				ID:        zone,
				Name:      "frontend",
				Subnet:    "10.70.0.0/24",
				OwnerKind: core.ZoneOwnerEnvironment,
				OwnerID:   environment,
			}}},
		DesiredRoutes: []environmentprojection.EnvironmentRouteProjection{{EnvironmentID: environment,
			Desired: core.Route{
				ID:              ids.New(ids.KindRoute),
				Host:            "old.example.test",
				Path:            "/",
				TargetServiceID: service,
				TargetPort:      8080,
				Exposure:        "public",
			}}}}
	action, err := sdk.NewActionDefinition("activate-config", sdk.CapabilityManagedConfig, sdk.OperationActivate)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := sdk.NewDefinition(
		sdk.DefinitionInput{Implementation: sdk.ImplementationKey(core.ComponentKindIngressCaddy),
			ConfigVariant: "test", Provides: []sdk.Capability{sdk.CapabilityServices, sdk.CapabilityHTTPRouter},
			OwnerScopes: []sdk.OwnerScope{sdk.OwnerScopeEnvironment}, Actions: []sdk.ActionDefinition{action}},
	)
	if err != nil {
		t.Fatal(err)
	}
	tunnelDefinition, err := sdk.NewDefinition(
		sdk.DefinitionInput{Implementation: sdk.ImplementationKey(core.ComponentKindEdgeCloudflare),
			ConfigVariant: "test", Provides: []sdk.Capability{sdk.CapabilityServices}, OwnerScopes: []sdk.OwnerScope{sdk.OwnerScopeEnvironment}},
	)
	if err != nil {
		t.Fatal(err)
	}
	tunnelRegistration := componentrender.EnvironmentComponentRegistration{Kind: core.ComponentKindEdgeCloudflare,
		Definition: tunnelDefinition, CatalogDigest: sha256.Sum256([]byte("native-route-disabled-tunnel")),
		Plan: func(_ core.Environment, component core.Component) (sdk.EnvironmentPlan, error) {
			if component.Enabled {
				t.Fatal("Route render tried to enable its unrelated Tunnel")
			}
			return sdk.EnvironmentPlan{}, nil
		}}
	registration := componentrender.EnvironmentComponentRegistration{
		Kind:          core.ComponentKindIngressCaddy,
		Definition:    definition,
		CatalogDigest: sha256.Sum256([]byte("native-route-catalog")),
		ManagedConfiguration: &componentrender.EnvironmentManagedConfigurationRegistration{
			SourcePath: "components/router/config",
			ActionID:   "activate-config",
			Activation: registeredcatalog.ContainerConfigActivationCommand,
		},
	}
	registration.ProjectHTTPRouter = func(environment core.Environment, component core.Component) (sdk.HTTPRouterInput, error) {
		input := sdk.HTTPRouterInput{
			ComponentID:        component.ID,
			Enabled:            component.Enabled,
			GeneratedServiceID: component.GeneratedServices[0],
			Zones: []sdk.HTTPRouterZoneInput{
				{ID: zone, Name: "frontend", StaticIPv4: component.PinnedIPv4},
			},
			Origin: sdk.HTTPRouterOrigin{ServiceName: "caddy", URL: "http://caddy:80"},
		}
		for _, route := range environment.Routes {
			input.Routes = append(input.Routes, sdk.HTTPRoute{ID: route.ID, Host: route.Host, Path: route.Path,
				BackendServiceID: route.TargetServiceID, BackendServiceName: "api", TargetPort: route.TargetPort, Exposure: sdk.HTTPRouteExposure(route.Exposure)})
		}
		return input, nil
	}
	registration.PlanHTTPRouter = func(input sdk.HTTPRouterInput, component core.Component) (sdk.EnvironmentPlan, error) {
		content := []byte("# no routes\n")
		if len(input.Routes) != 0 {
			content = []byte(input.Routes[0].Host + "\n")
		}
		return sdk.EnvironmentPlan{Services: []sdk.ManagedService{{ID: component.GeneratedServices[0], Name: "caddy",
			Image: controllerTestOCIImage("docker.io/library/caddy"), NetworkMode: sdk.ManagedNetworkModeZones,
			Networks: []sdk.ManagedNetworkAttachment{
				{Name: "frontend", StaticIPv4: component.PinnedIPv4},
			}, Restart: "unless-stopped", Replicas: 1,
			Mounts: []sdk.ManagedMount{
				{Source: "components/router/config", Target: "/etc/router/config", ReadOnly: true},
			}}},
			Files: []sdk.ManagedFile{{Path: "components/router/config", Content: content}}}, nil
	}
	registration.Plan = func(environment core.Environment, component core.Component) (sdk.EnvironmentPlan, error) {
		input, err := registration.ProjectHTTPRouter(environment, component)
		if err != nil {
			return sdk.EnvironmentPlan{}, err
		}
		return registration.PlanHTTPRouter(input, component)
	}
	volumeDir := "/var/lib/groundplane/vol/" + tenant + "/" + project + "/" + environment
	composeProject := &compose.Project{
		Services: compose.Services{"api": {Name: "api", Image: "app:captured", Expose: []string{"8080"},
			Networks: map[string]*compose.ServiceNetworkConfig{
				"frontend": {},
			}}},
		Networks: compose.Networks{"frontend": {Name: "frontend",
			Ipam: compose.IPAMConfig{Config: []*compose.IPAMPool{{Subnet: "10.70.0.0/24"}}}}},
	}
	projected.NormalizedCompose, err = composeProject.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	composeProject.Services["caddy"] = compose.ServiceConfig{
		Name:  "caddy",
		Image: controllerTestOCIImageReference("docker.io/library/caddy"),
		Networks: map[string]*compose.ServiceNetworkConfig{
			"frontend": {Ipv4Address: "10.70.0.2"},
		},
		Restart: "unless-stopped",
	}
	image, platform, reference := controllerTestOCIPlatform("docker.io/library/caddy")
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{Project: composeProject,
		ArtifactID: ids.New(ids.KindConfig), ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID: tenant, ProjectID: project, EnvironmentID: environment, PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		AuthorizedVolumeDir: volumeDir, Identities: composeidentity.Snapshot{Services: []composeidentity.Resource{
			{ID: service, Name: "api"}, {ID: router, Name: "caddy", ComponentID: component,
				ComponentImage: &composeidentity.ComponentImage{
					Repository:  image.Repository,
					IndexDigest: image.IndexDigest,
					Reference:   reference,
					Platform:    platform,
				}}},
			Networks: []composeidentity.Resource{{ID: zone, Name: "frontend"}}}})
	if err != nil {
		t.Fatal(err)
	}
	projected.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return nativeRouteFixture{projection: projected,
		catalog: []componentrender.EnvironmentComponentRegistration{
			registration,
			tunnelRegistration,
		}, state: &nativeRouteState{revision: 43},
		project: hierarchy.ProjectRecord{
			ID:       project,
			TenantID: tenant,
			Slug:     "project",
			Name:     "Project",
			Kind:     hierarchy.ProjectKindTenant,
		},
		environment: hierarchy.EnvironmentRecord{
			ID:                environment,
			ProjectID:         project,
			Name:              "qa",
			NetworkPool:       "10.70.0.0/16",
			VolumeDir:         volumeDir,
			ProvisioningState: hierarchy.EnvironmentProvisioningReady,
		}}
}

type nativeRouteState struct {
	revision, returnedRevision int64
	reads                      []int64
}

func (state *nativeRouteState) SnapshotRevision(context.Context) (int64, error) {
	return state.revision, nil
}

func (state *nativeRouteState) GetRouteProviderRuntimeAtRevision(
	_ context.Context,
	_ string,
	revision int64,
) (keyvalue.Versioned[componentplanning.Runtime], bool, error) {
	state.reads = append(state.reads, revision)
	if state.returnedRevision != 0 {
		revision = state.returnedRevision
	}
	return keyvalue.Versioned[componentplanning.Runtime]{ReadRevision: revision}, false, nil
}

type nativeRouteContents struct{ t *testing.T }

func (source nativeRouteContents) Load(context.Context, taskmaterialization.Record, uint64) ([]byte, error) {
	source.t.Fatal("command activation unexpectedly loaded mutable file content")
	return nil, nil
}

func nativeRouteWork(fixture nativeRouteFixture, action workexecution.Action) (workexecution.Task, workexecution.Item) {
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor, Action: action,
		TargetID: fixture.projection.DesiredRoutes[0].Desired.ID, TargetName: "route", ExpectedItems: 1,
		Scope: workexecution.Scope{
			TenantID:      fixture.project.TenantID,
			ProjectID:     fixture.project.ID,
			EnvironmentID: fixture.environment.ID,
		},
		Intent: workexecution.Input{
			ID:     ids.New(ids.KindPlan),
			SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute)}
	host := ids.New(ids.KindHost)
	item := workexecution.Item{ID: ids.New(ids.KindWorkItem), TaskID: task.ID, Operation: workexecution.Component,
		TargetID: fixture.projection.Components[0].Desired.ID, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: host},
		Phase: workexecution.Waiting, Cleanup: workexecution.CleanupNone, Wait: &workexecution.Wait{Reason: workexecution.WaitInput},
		Claims: []workexecution.Claim{
			{HostID: host, ScopeID: fixture.environment.ID},
		}, CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline}
	return task, item
}
