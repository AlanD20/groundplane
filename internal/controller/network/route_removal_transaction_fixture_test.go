package network

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testcomponentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testedgechecks "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testmaterializationcontent "github.com/AlanD20/groundplane/internal/infra/etcd/materializationcontent"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	runtimeconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

type routeRemovalTransactionStore struct {
	testkeyvalue.Store

	routeID               string
	environmentID         string
	indexRevision         int64
	revision              int64
	conflict              bool
	configuration         map[string]testkeyvalue.KeyValue
	configurationRevision int64
	source                *routeRemovalServiceEvidenceStore
}

// Model acknowledged configuration and immutable staging, not a second Route
// planner. The production publisher still owns the final deletion transaction.
func (store *routeRemovalTransactionStore) seedConfiguration(
	ctx context.Context, projection *testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection],
) error {
	if store.configuration == nil {
		store.configuration = make(map[string]testkeyvalue.KeyValue)
	}
	store.configurationRevision = 1
	sources, err := runtimeconfiguration.New(store)
	if err != nil {
		return err
	}
	reference, err := sources.Stage(ctx, runtimeconfiguration.Snapshot{
		ID: ids.New(ids.KindConfig), EnvironmentID: store.environmentID,
		Generation: projection.Record.RenderGeneration, Files: []testtaskmaterialization.Record{},
	})
	if err != nil {
		return err
	}
	value, err := runtimeconfiguration.EncodeReference(reference)
	if err != nil {
		return err
	}
	headKey := "/v1/runtime/environment-configurations/" + store.environmentID
	store.configuration[headKey] = testkeyvalue.KeyValue{
		Key:         headKey,
		Value:       value,
		ModRevision: store.configurationRevision,
	}
	value, err = testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection.Record)
	if err != nil {
		return err
	}
	appliedKey := "/v1/records/environment-compose-projections/" + store.environmentID
	store.configuration[appliedKey] = testkeyvalue.KeyValue{
		Key:         appliedKey,
		Value:       value,
		ModRevision: projection.Revision,
	}
	store.configurationRevision = projection.ReadRevision
	return nil
}

func (store *routeRemovalTransactionStore) seedTenant(
	tenant testkeyvalue.Versioned[testhierarchy.TenantRecord],
) error {
	value, err := testhierarchy.EncodeTenant(tenant.Record)
	if err != nil {
		return err
	}
	key := testhierarchy.TenantKey(tenant.Record.ID)
	store.configuration[key] = testkeyvalue.KeyValue{
		Key: key, Value: value, ModRevision: tenant.Revision,
	}
	return nil
}

func (store *routeRemovalTransactionStore) Get(
	_ context.Context, key string,
) (*testkeyvalue.GetResult, error) {
	value, found := store.configuration[key]
	if !found {
		return &testkeyvalue.GetResult{ReadRevision: store.configurationRevision}, nil
	}
	value.Value = append([]byte(nil), value.Value...)
	return &testkeyvalue.GetResult{Entry: &value, ReadRevision: store.configurationRevision}, nil
}

func (store *routeRemovalTransactionStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if store.source != nil && len(request.Keys) != 0 &&
		(strings.HasPrefix(request.Keys[0], "/v1/records/environment-blueprints/") ||
			strings.HasPrefix(request.Keys[0], "/v1/records/environment-blueprint-revisions/")) {
		return store.source.GetMany(context.Background(), request)
	}
	if len(request.Keys) != 0 && routeRemovalPlannerStorageKey(request.Keys[0]) {
		revision := request.Revision
		if revision == 0 {
			revision = store.configurationRevision
		}
		values := make([]*testkeyvalue.KeyValue, len(request.Keys))
		for index, key := range request.Keys {
			if value, found := store.configuration[key]; found && value.ModRevision <= revision {
				value.Value = append([]byte(nil), value.Value...)
				values[index] = &value
			}
		}
		return &testkeyvalue.GetManyResult{
			Values:           values,
			ReadRevision:     revision,
			ResponseRevision: store.configurationRevision,
		}, nil
	}
	return &testkeyvalue.GetManyResult{
		Values: []*testkeyvalue.KeyValue{
			{Value: []byte(store.routeID), ModRevision: store.indexRevision},
			{Value: []byte(store.routeID), ModRevision: store.indexRevision + 1},
		},
		ReadRevision: store.revision, ResponseRevision: store.revision,
	}, nil
}

func (store *routeRemovalTransactionStore) Range(
	_ context.Context, request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	// This fixture has neither external Route references nor active configuration
	// writers. The real planner still checks both at the captured revision.
	if request.Revision <= 0 || request.Prefix != testedgechecks.RouteReferencePrefix(store.routeID) &&
		request.Prefix != testtaskjournal.TaskEnvironmentIndexPrefix+store.environmentID+"/" {
		return nil, fmt.Errorf(
			"unexpected Route removal reference read: prefix %q revision %d",
			request.Prefix,
			request.Revision,
		)
	}
	return &testkeyvalue.RangeResult{
		ReadRevision: request.Revision, ResponseRevision: store.revision,
	}, nil
}

func (store *routeRemovalTransactionStore) Transact(
	_ context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if len(mutations) != 0 && routeRemovalPlannerStorageKey(mutations[0].Key) {
		for _, condition := range conditions {
			if condition.Prefix {
				for key := range store.configuration {
					if strings.HasPrefix(key, condition.Key) {
						return testkeyvalue.TransactionResult{Revision: store.configurationRevision}, nil
					}
				}
				continue
			}
			value, present := store.configuration[condition.Key]
			if present != (condition.ModRevision != 0) || present && value.ModRevision != condition.ModRevision {
				return testkeyvalue.TransactionResult{Revision: store.configurationRevision}, nil
			}
		}
		store.configurationRevision++
		for _, mutation := range mutations {
			if mutation.Type == testkeyvalue.MutationDelete {
				delete(store.configuration, mutation.Key)
			} else {
				store.configuration[mutation.Key] = testkeyvalue.KeyValue{Key: mutation.Key,
					Value: append([]byte(nil), mutation.Value...), ModRevision: store.configurationRevision}
			}
		}
		return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.configurationRevision}, nil
	}
	if !store.conflict {
		return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
	}
	failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
	for index, condition := range conditions {
		if condition.ModRevision == 0 {
			continue
		}
		revision := condition.ModRevision
		if strings.HasPrefix(condition.Key, "/v1/records/environments/") {
			revision++
		}
		failureReads[index] = &testkeyvalue.KeyValue{
			Key: condition.Key, Value: []byte(store.routeID), ModRevision: revision,
		}
	}
	return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: failureReads}, nil
}

func (store *routeRemovalTransactionStore) TransactBlueprintChild(
	ctx context.Context, transaction etcd.BlueprintChildTransaction,
) (testkeyvalue.TransactionResult, error) {
	conditions, mutations, err := transaction.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	return store.Transact(ctx, conditions, mutations)
}

func (*routeRemovalTransactionStore) ValidateBlueprintTaskTerminal(
	_ context.Context, transaction etcd.BlueprintTaskTerminalTransaction,
) error {
	return transaction.ValidateBudget("/groundplane/")
}

func (store *routeRemovalTransactionStore) TransactBlueprintTaskTerminal(
	ctx context.Context, transaction etcd.BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	conditions, mutations, err := transaction.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	return store.Transact(ctx, conditions, mutations)
}

func routeRemovalPlannerStorageKey(key string) bool {
	return strings.Contains(key, "/runtime-configuration-sources/") ||
		strings.Contains(key, "/component-materialization-content/") ||
		strings.HasPrefix(key, "/v1/runtime/environment-configurations/") ||
		strings.HasPrefix(key, "/v1/records/environment-compose-projections/") ||
		strings.HasPrefix(key, "/v1/records/tenants/")
}

func (fake *routeRemovalRepositoryFake) GetTenant(
	context.Context, string,
) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return fake.tenant, nil
}

func (*routeRemovalRepositoryFake) GetEnvironmentBlueprintRevision(
	context.Context, string, string,
) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision], bool, error) {
	return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]{}, false, nil
}

func (fake *routeRemovalRepositoryFake) GetEnvironmentComposeProjectionRevision(
	_ context.Context, environmentID string, revisionID string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	if fake.projection == nil || fake.projection.Record.EnvironmentID != environmentID ||
		fake.projection.Record.RevisionID != revisionID {
		return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
	}
	return *fake.projection, true, nil
}

func (fake *routeRemovalRepositoryFake) SnapshotRevision(context.Context) (int64, error) {
	return fake.projection.ReadRevision, nil
}

func (*routeRemovalRepositoryFake) GetRouteProviderRuntimeAtRevision(
	_ context.Context, _ string, revision int64,
) (testkeyvalue.Versioned[testcomponentplanning.Runtime], bool, error) {
	return testkeyvalue.Versioned[testcomponentplanning.Runtime]{ReadRevision: revision}, false, nil
}

func (fake *routeRemovalRepositoryFake) ListRoutes(
	context.Context, string, testkeyvalue.PageRequest,
) (testkeyvalue.Page[testroutes.Record], error) {
	return testkeyvalue.Page[testroutes.Record]{Revision: fake.projection.ReadRevision}, nil
}

func (fake *routeRemovalRepositoryFake) ListServices(
	context.Context, string, testkeyvalue.PageRequest,
) (testkeyvalue.Page[testservices.ServiceRecord], error) {
	return testkeyvalue.Page[testservices.ServiceRecord]{Revision: fake.projection.ReadRevision}, nil
}

func (fake *routeRemovalRepositoryFake) ListZones(
	context.Context, string, testkeyvalue.PageRequest,
) (testkeyvalue.Page[testzones.Record], error) {
	return testkeyvalue.Page[testzones.Record]{Revision: fake.projection.ReadRevision}, nil
}

func newRouteRemovalPlanFake(
	t *testing.T, repository *routeRemovalRepositoryFake,
) *routeRemovalPlanFake {
	t.Helper()
	resolver, err := testtaskplanning.NewTaskPlanResolverWithBlueprints(
		"/var/lib/groundplane/vol", repository, routeRemovalTestComponentCatalog(t),
	)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	content, err := testmaterializationcontent.New(repository.transactionStore)
	if err != nil {
		t.Fatalf("materializationcontent.New() error = %v", err)
	}
	if err := resolver.EnableComponentMaterializationContent(content); err != nil {
		t.Fatalf("EnableComponentMaterializationContent() error = %v", err)
	}
	if err := resolver.EnableRoutePlans(repository); err != nil {
		t.Fatalf("EnableRoutePlans() error = %v", err)
	}
	tasks, err := etcd.NewTaskRepository(repository.transactionStore)
	if err != nil {
		t.Fatalf("NewTaskRepository() error = %v", err)
	}
	ledger, err := etcd.NewReleaseLedger(repository.transactionStore, tasks)
	if err != nil {
		t.Fatalf("NewReleaseLedger() error = %v", err)
	}
	if err := resolver.EnableReleasePlans(ledger); err != nil {
		t.Fatalf("EnableReleasePlans() error = %v", err)
	}
	sources, err := runtimeconfiguration.New(repository.transactionStore)
	if err != nil {
		t.Fatalf("runtimeconfiguration.New() error = %v", err)
	}
	if err := resolver.EnableConfigurationRecovery(sources); err != nil {
		t.Fatalf("EnableConfigurationRecovery() error = %v", err)
	}
	return &routeRemovalPlanFake{resolver: resolver}
}

type routeRemovalRouterRenderer struct{}

func (routeRemovalRouterRenderer) Plan(
	environment core.Environment, component core.Component,
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
			ID: component.GeneratedServices[0], Name: "caddy", Image: routeRemovalTestOCIImage(),
			NetworkMode: componentsdk.ManagedNetworkModeZones,
			Networks: []componentsdk.ManagedNetworkAttachment{{
				Name: "frontend", StaticIPv4: component.PinnedIPv4,
			}},
			Restart: "unless-stopped", Replicas: 1,
			Mounts: []componentsdk.ManagedMount{{
				Source: "components/router/config", Target: "/etc/router/config", ReadOnly: true,
			}},
		}},
		Files: []componentsdk.ManagedFile{{Path: "components/router/config", Content: content}},
	}, nil
}

func (routeRemovalRouterRenderer) ProjectHTTPRouter(
	environment core.Environment, component core.Component,
) (componentsdk.HTTPRouterInput, error) {
	zone := environment.Zones["frontend"]
	input := componentsdk.HTTPRouterInput{
		ComponentID: component.ID, Enabled: true, GeneratedServiceID: component.GeneratedServices[0],
		Zones: []componentsdk.HTTPRouterZoneInput{{
			ID: zone.ID, Name: zone.Name, StaticIPv4: component.PinnedIPv4,
		}},
		Origin: componentsdk.HTTPRouterOrigin{ServiceName: "caddy", URL: "http://caddy:80"},
	}
	for _, route := range environment.Routes {
		backend := environment.Services["api"]
		input.Routes = append(input.Routes, componentsdk.HTTPRoute{
			ID: route.ID, Host: route.Host, Path: route.Path,
			BackendServiceID: route.TargetServiceID, BackendServiceName: backend.Name,
			TargetPort: route.TargetPort, Exposure: componentsdk.HTTPRouteExposure(route.Exposure),
		})
	}
	return input, nil
}

func (renderer routeRemovalRouterRenderer) PlanHTTPRouter(
	input componentsdk.HTTPRouterInput, component core.Component,
) (componentsdk.EnvironmentPlan, error) {
	environment := core.Environment{Routes: make([]core.Route, len(input.Routes))}
	for index, route := range input.Routes {
		environment.Routes[index] = core.Route{
			ID: route.ID, Host: route.Host, Path: route.Path,
			TargetServiceID: route.BackendServiceID, TargetPort: route.TargetPort,
			Exposure: string(route.Exposure),
		}
	}
	return renderer.Plan(environment, component)
}

func routeRemovalTestComponentCatalog(
	t *testing.T,
) []testcomponentrender.EnvironmentComponentRegistration {
	t.Helper()
	action, err := componentsdk.NewActionDefinition(
		componentsdk.ActionID("activate-config"),
		componentsdk.CapabilityManagedConfig,
		componentsdk.OperationActivate,
	)
	if err != nil {
		t.Fatalf("NewActionDefinition() error = %v", err)
	}
	routerDefinition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: componentsdk.ImplementationKey(core.ComponentKindIngressCaddy),
		ConfigVariant:  componentsdk.ConfigVariant("test"),
		Provides:       []componentsdk.Capability{componentsdk.CapabilityServices, componentsdk.CapabilityHTTPRouter},
		OwnerScopes:    []componentsdk.OwnerScope{componentsdk.OwnerScopeEnvironment},
		Actions:        []componentsdk.ActionDefinition{action},
	})
	if err != nil {
		t.Fatalf("NewDefinition(router) error = %v", err)
	}
	edgeDefinition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: componentsdk.ImplementationKey(core.ComponentKindEdgeCloudflare),
		ConfigVariant:  componentsdk.ConfigVariant("test"),
		Provides:       []componentsdk.Capability{componentsdk.CapabilityServices},
		OwnerScopes:    []componentsdk.OwnerScope{componentsdk.OwnerScopeEnvironment},
	})
	if err != nil {
		t.Fatalf("NewDefinition(edge) error = %v", err)
	}
	renderer := routeRemovalRouterRenderer{}
	return []testcomponentrender.EnvironmentComponentRegistration{
		{
			Kind: core.ComponentKindIngressCaddy, Definition: routerDefinition,
			CatalogDigest: sha256.Sum256([]byte("route removal caddy catalog")),
			Plan:          renderer.Plan, ProjectHTTPRouter: renderer.ProjectHTTPRouter,
			PlanHTTPRouter: renderer.PlanHTTPRouter,
			ManagedConfiguration: &testcomponentrender.EnvironmentManagedConfigurationRegistration{
				SourcePath: "components/router/config", ActionID: componentsdk.ActionID("activate-config"),
				Activation: registeredcatalog.ContainerConfigActivationCommand,
			},
		},
		{
			Kind: core.ComponentKindEdgeCloudflare, Definition: edgeDefinition,
			CatalogDigest: sha256.Sum256([]byte("route removal edge catalog")),
			Plan: func(core.Environment, core.Component) (componentsdk.EnvironmentPlan, error) {
				return componentsdk.EnvironmentPlan{}, nil
			},
		},
	}
}

func routeRemovalTestOCIImage() componentsdk.OCIImage {
	return componentsdk.OCIImage{
		Repository: "docker.io/library/caddy", IndexDigest: strings.Repeat("a", 64),
		Platforms: []componentsdk.OCIPlatform{
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  strings.Repeat("b", 64),
				ConfigDigest: strings.Repeat("d", 64),
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  strings.Repeat("c", 64),
				ConfigDigest: strings.Repeat("e", 64),
			},
		},
	}
}

func routeRemovalTestRouterNormalizedCompose(t *testing.T) []byte {
	t.Helper()
	project := &composetypes.Project{
		Services: composetypes.Services{
			"api": {
				Name: "api", Image: "api:1",
				Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
			},
		},
		Networks: composetypes.Networks{
			"frontend": {
				Name: "frontend",
				Ipam: composetypes.IPAMConfig{Config: []*composetypes.IPAMPool{{Subnet: "10.70.1.0/24"}}},
			},
		},
	}
	normalized, err := testcomposerender.MarshalNormalizedEnvironmentProject(project)
	if err != nil {
		t.Fatalf("marshal Route removal router fixture normalized Compose: %v", err)
	}
	return normalized
}
