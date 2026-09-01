package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type routeRemovalPlanReader struct {
	*blueprintPlanReader
	intent         etcd.RouteRemovalIntent
	mutationIntent etcd.RouteMutationIntent
}

func (reader *routeRemovalPlanReader) SnapshotRevision(context.Context) (int64, error) {
	return 20, nil
}

func (reader *routeRemovalPlanReader) ListRoutes(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	return etcd.Page[etcd.RouteRecord]{Revision: 20}, nil
}

func (reader *routeRemovalPlanReader) ListServices(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return etcd.Page[etcd.ServiceRecord]{Revision: 20}, nil
}

func (reader *routeRemovalPlanReader) ListZones(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	return etcd.Page[etcd.ZoneRecord]{Revision: 20}, nil
}

type routeRemovalPlanProviderRenderer struct{}

func (routeRemovalPlanProviderRenderer) Plan(
	environment core.Environment,
	component core.Component,
) (componentsdk.EnvironmentPlan, error) {
	if !component.Enabled {
		return componentsdk.EnvironmentPlan{}, nil
	}
	content := []byte("# no routes\n")
	if len(environment.Routes) != 0 {
		content = []byte(environment.Routes[0].Host + "\n")
	}
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{{
			ID: component.GeneratedServices[0], Name: "caddy", Image: controllerTestOCIImage("docker.io/library/caddy"),
			NetworkMode: componentsdk.ManagedNetworkModeZones,
			Networks:    []componentsdk.ManagedNetworkAttachment{{Name: "frontend", StaticIPv4: component.PinnedIPv4}},
			Restart:     "unless-stopped", Replicas: 1,
			Mounts: []componentsdk.ManagedMount{{
				Source: "components/router/config", Target: "/etc/router/config", ReadOnly: true,
			}},
		}},
		Files: []componentsdk.ManagedFile{{Path: "components/router/config", Content: content}},
	}, nil
}

func (routeRemovalPlanProviderRenderer) ProjectHTTPRouter(
	environment core.Environment,
	component core.Component,
) (componentsdk.HTTPRouterInput, error) {
	input := componentsdk.HTTPRouterInput{
		ComponentID: component.ID, Enabled: true, GeneratedServiceID: component.GeneratedServices[0],
		ZoneID: ids.New(ids.KindNetwork), ZoneName: "frontend", PinnedIPv4: component.PinnedIPv4,
		Origin: componentsdk.HTTPRouterOrigin{ServiceName: "caddy", URL: "http://caddy:80"},
	}
	for _, route := range environment.Routes {
		input.Routes = append(input.Routes, componentsdk.HTTPRoute{
			ID: route.ID, Host: route.Host, Path: route.Path, BackendServiceID: route.TargetServiceID,
			BackendServiceName: "backend", TargetPort: route.TargetPort,
			Exposure: componentsdk.HTTPRouteExposure(route.Exposure),
		})
	}
	return input, nil
}

func (renderer routeRemovalPlanProviderRenderer) PlanHTTPRouter(
	input componentsdk.HTTPRouterInput,
	component core.Component,
) (componentsdk.EnvironmentPlan, error) {
	environment := core.Environment{Routes: make([]core.Route, len(input.Routes))}
	for index, route := range input.Routes {
		environment.Routes[index] = core.Route{ID: route.ID, Host: route.Host, Path: route.Path,
			TargetServiceID: route.BackendServiceID, TargetPort: route.TargetPort, Exposure: string(route.Exposure)}
	}
	return renderer.Plan(environment, component)
}

func (reader *routeRemovalPlanReader) GetRouteRemovalIntent(
	context.Context,
	string,
) (etcd.Versioned[etcd.RouteRemovalIntent], bool, error) {
	return etcd.Versioned[etcd.RouteRemovalIntent]{Record: reader.intent}, true, nil
}

func (reader *routeRemovalPlanReader) GetRouteMutationIntent(
	context.Context,
	string,
) (etcd.Versioned[etcd.RouteMutationIntent], bool, error) {
	return etcd.Versioned[etcd.RouteMutationIntent]{Record: reader.mutationIntent}, true, nil
}

// Rationale: initial publication and restart-time reconstruction must seal the
// same candidate managed configuration metadata, exact generated Service, and closed
// validate/reload procedure without persisting generated file bytes.
func TestTaskPlanResolverRebuildsRouteRemovalProviderProcedure(t *testing.T) {
	t.Parallel()
	reader, intent, task := routeRemovalPlanTestState(t)
	_, _, _, _, catalog := componentPlanProjectionInput(t)
	definition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: catalog[0].Definition.Implementation(), ConfigVariant: catalog[0].Definition.ConfigVariant(),
		Provides:    []componentsdk.Capability{componentsdk.CapabilityServices, componentsdk.CapabilityHTTPRouter},
		OwnerScopes: catalog[0].Definition.OwnerScopes(), Actions: catalog[0].Definition.Actions(),
	})
	if err != nil {
		t.Fatalf("NewDefinition(router provider) error = %v", err)
	}
	catalog[0].Definition = definition
	catalog[0].Plan = routeRemovalPlanProviderRenderer{}.Plan
	catalog[0].ProjectHTTPRouter = routeRemovalPlanProviderRenderer{}.ProjectHTTPRouter
	catalog[0].PlanHTTPRouter = routeRemovalPlanProviderRenderer{}.PlanHTTPRouter
	catalog[0].ManagedConfiguration = &EnvironmentManagedConfigurationRegistration{
		SourcePath: "components/router/config", ActionID: componentsdk.ActionID("activate-config"),
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, catalog)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	if err := resolver.EnableRoutePlans(reader); err != nil {
		t.Fatalf("EnableRoutePlans() error = %v", err)
	}
	prepared, err := resolver.PrepareRouteRemovalTask(
		context.Background(),
		task,
		intent,
		RouteRemovalTaskProcedureIDs{
			ArtifactID:         "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			MaterializationID:  "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			MaterializeStepID:  "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			ComposeApplyStepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			ActivateStepID:     "step_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		},
	)
	if err != nil {
		t.Fatalf("PrepareRouteRemovalTask() error = %v", err)
	}
	reader.intent = prepared.Intent
	first, err := resolver.ResolveExecutionPlan(context.Background(), prepared.Task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), prepared.Task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(replay) error = %v", err)
	}
	digest, err := hex.DecodeString(prepared.Task.Materializations[0].SHA256)
	if err != nil {
		t.Fatalf("DecodeString(materialization digest) error = %v", err)
	}
	composeApply := first.Steps[1].GetComposeApply()
	apply := first.Steps[2].GetComponentApply()
	componentServiceOwned := false
	for _, service := range first.Artifacts[0].GetServices() {
		if service.GetServiceId() == intent.CandidateProjection.Components[0].Runtime.GeneratedServices[0] &&
			service.GetOwnerComponentId() == intent.CandidateProjection.Components[0].Desired.ID {
			componentServiceOwned = true
		}
	}
	if !bytes.Equal(first.PlanHash, second.PlanHash) || hex.EncodeToString(first.PlanHash) != prepared.Task.PlanHash ||
		first.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || first.TargetId != task.Target ||
		len(first.Artifacts) != 1 || len(first.Steps) != 3 || first.Steps[0].GetMaterializeFile() == nil ||
		composeApply == nil || len(composeApply.GetServiceIds()) != 1 ||
		!composeApply.GetForceRecreate() || !composeApply.GetNoDependencies() ||
		composeApply.GetServiceIds()[0] != intent.CandidateProjection.Components[0].Runtime.GeneratedServices[0] ||
		first.Steps[1].GetPrerequisiteStepId() != first.Steps[0].GetStepId() ||
		first.Steps[2].GetPrerequisiteStepId() != first.Steps[1].GetStepId() ||
		apply == nil || !componentServiceOwned ||
		apply.ActionId != "activate-config" || !bytes.Equal(apply.ArtifactDigest, digest) {
		t.Fatalf("resolved Route removal plans = %#v / %#v", first, second)
	}
	content, err := resolver.ResolveComponentFile(
		context.Background(),
		intent.EnvironmentID,
		*prepared.Task.Materializations[0].Source.ComponentFile,
	)
	if err != nil {
		t.Fatalf("ResolveComponentFile(candidate) error = %v", err)
	}
	if strings.Contains(string(content), "app.example.com") {
		t.Fatalf("candidate managed configuration retained removed Route: %q", content)
	}
}

func TestTaskPlanResolverPinsAndRebuildsRouteMutationProviderProcedure(t *testing.T) {
	t.Parallel()
	reader, _, baseTask := routeRemovalPlanTestState(t)
	_, _, _, _, catalog := componentPlanProjectionInput(t)
	projection := reader.blueprintPlanReader.projection
	definition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: catalog[0].Definition.Implementation(), ConfigVariant: catalog[0].Definition.ConfigVariant(),
		Provides:    []componentsdk.Capability{componentsdk.CapabilityServices, componentsdk.CapabilityHTTPRouter},
		OwnerScopes: catalog[0].Definition.OwnerScopes(), Actions: catalog[0].Definition.Actions(),
	})
	if err != nil {
		t.Fatalf("NewDefinition(router provider) error = %v", err)
	}
	catalog[0].Definition = definition
	catalog[0].Plan = routeRemovalPlanProviderRenderer{}.Plan
	catalog[0].ProjectHTTPRouter = routeRemovalPlanProviderRenderer{}.ProjectHTTPRouter
	catalog[0].PlanHTTPRouter = routeRemovalPlanProviderRenderer{}.PlanHTTPRouter
	catalog[0].ManagedConfiguration = &EnvironmentManagedConfigurationRegistration{
		SourcePath: "components/router/config", ActionID: "activate-config",
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, catalog)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	if err := resolver.EnableRoutePlans(reader); err != nil {
		t.Fatalf("EnableRoutePlans() error = %v", err)
	}
	route, err := etcd.NewRouteRecord(projection.EnvironmentID, projection.DesiredRoutes[0].Desired)
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	current := etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: projection, Revision: 18, ReadRevision: 18}
	intent, err := etcd.NewRouteMutationIntent(
		baseTask.ID, baseTask.OperationID, projection.EnvironmentID, route, nil, &current, baseTask.CreatedAt,
	)
	if err != nil {
		t.Fatalf("NewRouteMutationIntent() error = %v", err)
	}
	task := baseTask
	task.Type = etcd.TaskCreate
	task.Target = route.Desired.ID
	prepared, err := resolver.PrepareRouteMutationTask(
		context.Background(),
		task,
		intent,
		etcd.RouteMutationProcedureIDs{
			ArtifactID: ids.New(ids.KindConfig), MaterializationID: ids.New(ids.KindConfig),
			MaterializeStepID: ids.New(ids.KindStep), ApplyStepID: ids.New(ids.KindStep),
			ActivateStepID: ids.New(ids.KindStep),
		},
	)
	if err != nil {
		t.Fatalf("PrepareRouteMutationTask() error = %v", err)
	}
	if prepared.Intent.Provider == nil || len(prepared.Intent.Provider.Input.Routes) != 1 ||
		prepared.Intent.Route.Observed.Status != etcd.RouteObservedPending ||
		prepared.Task.Executor != etcd.TaskExecutorAgent {
		t.Fatalf("prepared Route mutation = %#v / %#v", prepared.Intent, prepared.Task)
	}
	reader.mutationIntent = prepared.Intent
	first, err := resolver.ResolveExecutionPlan(context.Background(), prepared.Task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), prepared.Task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(replay) error = %v", err)
	}
	if !bytes.Equal(first.PlanHash, second.PlanHash) || hex.EncodeToString(first.PlanHash) != prepared.Task.PlanHash {
		t.Fatal("Route mutation replay hash changed")
	}
}

func routeRemovalPlanTestState(
	t *testing.T,
) (*routeRemovalPlanReader, etcd.RouteRemovalIntent, etcd.TaskRecord) {
	t.Helper()
	project, identity, projection, routeSpecs, catalog := componentPlanProjectionInput(t)
	identity.TenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	identity.TenantSlug = "acme"
	identity.ProjectSlug = "shop"
	at := time.Date(2026, 8, 23, 4, 0, 0, 0, time.UTC)
	projection.DesiredRoutes[0].DesiredGeneration = 1
	catalog[0].Plan = routeRemovalPlanProviderRenderer{}.Plan
	componentProjection, err := projectPinnedEnvironmentComponents(
		project,
		nil,
		identity,
		projection,
		routeSpecs,
		map[string]core.ComponentSpec{string(core.ComponentCapabilityHTTPRouter): {
			Implementation: core.ComponentKindIngressCaddy,
			Enabled:        true,
			Settings: core.ComponentCapabilitySettings{
				ZoneID: projection.DesiredZones[0].Desired.ID,
			},
		}},
		nil,
		catalog,
	)
	if err != nil {
		t.Fatalf("project Route removal fixture components: %v", err)
	}
	identities := ComposeIdentitySnapshot{
		Services: append([]ComposeResourceIdentity(nil), componentProjection.Services...),
		Networks: desiredZoneResourceIdentities(projection.DesiredZones),
		Volumes:  composeVolumeResourceIdentities(projection.Volumes),
	}
	for _, service := range projection.DesiredServices {
		identities.Services = append(identities.Services, ComposeResourceIdentity{
			ID: service.Desired.ID, Name: service.Desired.Name,
		})
	}
	services, err := ProjectServiceProjection(componentProjection.Project, identities, projection.ServiceExtensions)
	if err != nil {
		t.Fatalf("project Route removal fixture services: %v", err)
	}
	projection.DesiredServices = make([]etcd.EnvironmentServiceProjection, len(services))
	for index, service := range services {
		projection.DesiredServices[index] = etcd.EnvironmentServiceProjection{
			EnvironmentID: projection.EnvironmentID,
			Desired:       service,
		}
	}
	sort.Slice(projection.DesiredServices, func(left, right int) bool {
		return projection.DesiredServices[left].Desired.ID < projection.DesiredServices[right].Desired.ID
	})
	normalized, err := MarshalNormalizedEnvironmentProject(componentProjection.Project)
	if err != nil {
		t.Fatalf("marshal Route removal fixture normalized Compose: %v", err)
	}
	projection.NormalizedCompose = normalized
	artifact, err := RenderCompose(ComposeRenderInput{
		Project: componentProjection.Project, ArtifactID: ids.NewAt(ids.KindConfig, at, 90),
		ProjectOwnerKind: ComposeProjectOwnerTenant,
		TenantID:         identity.TenantID, ProjectID: identity.ProjectID, EnvironmentID: identity.EnvironmentID,
		PlanID: ids.NewAt(ids.KindPlan, at, 91), RenderGeneration: projection.RenderGeneration,
		AuthorizedVolumeDir: identity.AuthorizedVolumeDir,
		Identities:          mustComposeIdentitySnapshotFromProjection(t, projection),
	})
	if err != nil {
		t.Fatalf("RenderCompose(Route removal fixture) error = %v", err)
	}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal Route removal fixture artifact: %v", err)
	}
	content := []byte(`kind: environment
schema: 1
metadata: {tenant: acme, project: shop, environment: production}
x-gp-network-pool: 10.70.0.0/16
services:
  api:
    image: api:1
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
		zone_id: ` + projection.DesiredZones[0].Desired.ID + "\n")
	base := &blueprintPlanReader{
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
			Files: []etcd.EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: content}}, CreatedAt: at,
		},
		projection: projection,
	}
	taskID := ids.NewAt(ids.KindTask, at, 100)
	intent, err := etcd.NewRouteRemovalIntent(
		taskID,
		identity.EnvironmentID,
		projection.DesiredRoutes[0].Desired.ID,
		17,
		&etcd.Versioned[etcd.EnvironmentComposeProjection]{
			Record: projection, Revision: 18, ReadRevision: 18,
		},
		at,
	)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	reader := &routeRemovalPlanReader{blueprintPlanReader: base, intent: intent}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, at, 101),
		Executor: etcd.TaskExecutorAgent, PlanID: ids.NewAt(ids.KindPlan, at, 102),
		Type: etcd.TaskRemove, Target: intent.RouteID, TimeoutSeconds: 120,
		Status: etcd.TaskStatusPending, CreatedAt: at,
	}
	return reader, intent, task
}
