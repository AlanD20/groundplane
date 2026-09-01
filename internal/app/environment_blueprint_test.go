package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

func TestEnvironmentBlueprintTopologyProjectionExcludesRuntimeAndObservation(t *testing.T) {
	// Rationale: the sealed desired revision is assembled from validated
	// publication candidates without copying Controller-owned runtime state.
	t.Parallel()
	at := time.Date(2026, 8, 31, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	zone, err := etcd.NewZoneRecord(environmentID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, at, 2), Name: "private", Subnet: "10.30.0.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	serviceRecord, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: ids.NewAt(ids.KindService, at, 3), Name: "api", Image: "example/api:1",
	}, "")
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	serviceRecord, err = etcd.SetServiceRuntimeIntent(serviceRecord, core.ServiceRuntimeIntentStopped)
	if err != nil {
		t.Fatalf("SetServiceRuntimeIntent() error = %v", err)
	}
	routeRecord, err := etcd.NewRouteRecord(environmentID, core.Route{
		ID: ids.NewAt(ids.KindRoute, at, 4), Host: "api.example.test", Path: "/",
		TargetServiceID: serviceRecord.Desired.ID, TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	routeDesired := routeRecord.Desired
	routeDesired.Exposure = "internal"
	routeRecord, err = etcd.ReplaceRouteDesired(routeRecord, routeDesired)
	if err != nil {
		t.Fatalf("ReplaceRouteDesired() error = %v", err)
	}
	routeRecord, err = etcd.SetRouteObservation(routeRecord, etcd.RouteObservation{
		Status: etcd.RouteObservedServed, DesiredGeneration: routeRecord.DesiredGeneration,
		Provider: etcd.RouteProviderObservation{
			ComponentID: ids.NewAt(ids.KindComponent, at, 5), DefinitionDigest: strings.Repeat("a", 64),
			CatalogDigest: strings.Repeat("b", 64), InputRevision: 7, InputGeneration: 8,
		},
	})
	if err != nil {
		t.Fatalf("SetRouteObservation() error = %v", err)
	}

	zones, services, routes := environmentBlueprintTopologyProjection(
		[]etcd.EnvironmentBlueprintZoneChange{{Record: zone}},
		[]etcd.EnvironmentBlueprintServiceChange{{Record: serviceRecord}},
		[]etcd.EnvironmentBlueprintRouteChange{{Record: routeRecord}},
	)
	wantZones := []etcd.EnvironmentZoneProjection{{EnvironmentID: environmentID, Desired: zone.Desired}}
	wantServices := []etcd.EnvironmentServiceProjection{{
		EnvironmentID: environmentID, BackingNetworkID: serviceRecord.BackingNetworkID, Desired: serviceRecord.Desired,
	}}
	wantRoutes := []etcd.EnvironmentRouteProjection{{
		EnvironmentID: environmentID, Desired: routeRecord.Desired, DesiredGeneration: routeRecord.DesiredGeneration,
	}}
	if !reflect.DeepEqual(zones, wantZones) || !reflect.DeepEqual(services, wantServices) ||
		!reflect.DeepEqual(routes, wantRoutes) {
		t.Fatalf("desired topology projection = %#v/%#v/%#v", zones, services, routes)
	}
}

// Rationale: idempotency must compare the verified logical Blueprint rather than unstable multipart framing or map order.
func TestEnvironmentBlueprintIntentManifestIsCanonical(t *testing.T) {
	bundle := core.BlueprintBundle{
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files:         []core.BlueprintFile{{Path: "blueprint.yaml", Content: []byte("services: {}\n")}},
		Interpolation: map[string]string{"ZED": "2", "ALPHA": "1"},
	}
	manifest, err := desiredrevision.IntentManifest(bundle)
	if err != nil {
		t.Fatalf("environmentBlueprintIntentManifest() error = %v", err)
	}
	if manifest.FormatVersion != 1 || manifest.Files[0].Part != "file-000001" ||
		!reflect.DeepEqual(manifest.Interpolation, []idempotentintent.Interpolation{
			{Name: "ALPHA", Value: "1"}, {Name: "ZED", Value: "2"},
		}) {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestPrepareEnvironmentBlueprintServiceChangesPreservesRuntimeIntent(t *testing.T) {
	// Rationale: Blueprint desired replacement must preserve a stopped Service's
	// Controller-owned runtime intent byte-for-byte.
	t.Parallel()
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	record, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "api", Image: "app:old",
	}, "")
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	record, err = etcd.SetServiceRuntimeIntent(record, core.ServiceRuntimeIntentStopped)
	if err != nil {
		t.Fatalf("SetServiceRuntimeIntent() error = %v", err)
	}
	current := etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 7, ReadRevision: 9}

	changes, err := prepareEnvironmentBlueprintServiceChanges(
		environmentID,
		[]core.Service{{ID: serviceID, Name: "api", Image: "app:new"}},
		[]etcd.Versioned[etcd.ServiceRecord]{current},
	)
	if err != nil {
		t.Fatalf("prepareEnvironmentBlueprintServiceChanges() error = %v", err)
	}
	if len(changes) != 1 || changes[0].Current == nil ||
		changes[0].Record.Desired.Image != "app:new" || changes[0].Record.Runtime != record.Runtime {
		t.Fatalf("changes = %#v", changes)
	}
}

func TestPrepareEnvironmentBlueprintServiceChangesStartsNewServiceRunning(t *testing.T) {
	// Rationale: a Service first introduced by Blueprint starts with the exact
	// running runtime intent accepted by ADR 0028.
	t.Parallel()
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	serviceID := ids.NewAt(ids.KindService, at, 4)

	changes, err := prepareEnvironmentBlueprintServiceChanges(
		environmentID,
		[]core.Service{{ID: serviceID, Name: "worker", Image: "app:1"}},
		nil,
	)
	if err != nil {
		t.Fatalf("prepareEnvironmentBlueprintServiceChanges() error = %v", err)
	}
	if len(changes) != 1 || changes[0].Current != nil ||
		changes[0].Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("changes = %#v", changes)
	}
}

func TestPrepareEnvironmentBlueprintRouteChangesPreservesImmutableTarget(t *testing.T) {
	// Rationale: Blueprint replacement may change Route exposure but must not
	// silently retarget an existing immutable host/path identity.
	t.Parallel()
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 5)
	routeID := ids.NewAt(ids.KindRoute, at, 6)
	serviceID := ids.NewAt(ids.KindService, at, 7)
	record, err := etcd.NewRouteRecord(environmentID, core.Route{
		ID: routeID, Host: "app.example.com", Path: "/app/*",
		TargetServiceID: serviceID, TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	current := etcd.Versioned[etcd.RouteRecord]{Record: record, Revision: 7, ReadRevision: 9}
	desired := record.Desired
	desired.Exposure = "internal"

	changes, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID,
		[]core.Route{desired},
		[]etcd.Versioned[etcd.RouteRecord]{current},
	)
	if err != nil {
		t.Fatalf("prepareEnvironmentBlueprintRouteChanges() error = %v", err)
	}
	if len(changes) != 1 || changes[0].Current == nil || changes[0].Record.Desired.Exposure != "internal" {
		t.Fatalf("changes = %#v", changes)
	}

	retargeted := desired
	retargeted.TargetServiceID = ids.NewAt(ids.KindService, at, 8)
	if _, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID,
		[]core.Route{retargeted},
		[]etcd.Versioned[etcd.RouteRecord]{current},
	); err == nil {
		t.Fatal("prepareEnvironmentBlueprintRouteChanges() accepted an immutable target change")
	}
}

func TestPreserveEnvironmentBlueprintResourcesCarriesForwardOmittedResources(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 21, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 10)
	networkID := ids.NewAt(ids.KindNetwork, at, 11)
	volumeID := ids.NewAt(ids.KindVolume, at, 12)
	project := &composetypes.Project{}
	prior := &composetypes.Project{Services: composetypes.Services{
		"api":       {Name: "api", Image: "example/api:1"},
		"api__blue": {Name: "api__blue", Image: "caddy:2"},
	}, Networks: composetypes.Networks{"backend": {}}, Volumes: composetypes.Volumes{"data": {}}}
	err := preserveEnvironmentBlueprintResources(project, prior, controller.ComposeIdentitySnapshot{
		Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: "api"}},
		Networks: []controller.ComposeResourceIdentity{{ID: networkID, Name: "backend"}},
	}, []etcd.EnvironmentVolumeIdentity{{ID: volumeID, Key: "data", Slug: "data"}}, true)
	if err != nil {
		t.Fatalf("preserveEnvironmentBlueprintResources() error = %v", err)
	}
	service, serviceFound := project.Services["api"]
	if !serviceFound || service.Name != "api" || service.Image != "example/api:1" {
		t.Fatalf("retained Service = %#v, found = %t", service, serviceFound)
	}
	if _, found := project.Networks["backend"]; !found {
		t.Fatal("retained Zone was omitted from candidate Compose project")
	}
	volume, volumeFound := project.Volumes["data"]
	if !volumeFound || volume.Extensions["x-gp-slug"] != "data" {
		t.Fatalf("retained Volume = %#v, found = %t", volume, volumeFound)
	}
}

func TestPreserveEnvironmentBlueprintResourcesUsesNormalizedProjectionAuthority(t *testing.T) {
	t.Parallel()
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const generatedServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FB0"
	const componentID = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	canonical := []byte(`services:
  api:
    image: example/api:1
  router:
    image: example/router:1
configs:
  app-config:
    file: config/app.conf
secrets:
  app-secret:
    file: secrets/app.secret
volumes:
  data:
    labels:
      com.example.owner: operator
`)
	authoredCanonical := []byte(`services:
  api:
    image: example/api:1
configs:
  app-config:
    file: config/app.conf
secrets:
  app-secret:
    file: secrets/app.secret
volumes:
  data:
    labels:
      com.example.owner: operator
`)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    environmentID, ProjectName: "groundplane-test", CanonicalYaml: canonical,
		Services: []*agentpb.ComposeService{
			{ServiceId: serviceID, ComposeName: "api"},
			{ServiceId: generatedServiceID, ComposeName: "router", OwnerComponentId: componentID},
		},
	})
	if err != nil {
		t.Fatalf("marshal normalized projection: %v", err)
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, ComposeArtifact: artifact, NormalizedCompose: authoredCanonical,
		DesiredServices: []etcd.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: core.Service{ID: serviceID, Name: "api"},
		}},
		Volumes: []etcd.EnvironmentVolumeIdentity{{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Key: "data", Slug: "data"}},
	}
	authored, err := authoredComposeIdentitySnapshot(projection)
	if err != nil {
		t.Fatalf("load authored identity snapshot: %v", err)
	}
	if !reflect.DeepEqual(authored.Services, []controller.ComposeResourceIdentity{{ID: serviceID, Name: "api"}}) {
		t.Fatalf("authored Service identities = %#v", authored.Services)
	}
	prior, err := controller.LoadNormalizedEnvironmentProject(context.Background(), projection)
	if err != nil {
		t.Fatalf("load normalized desired revision: %v", err)
	}
	candidate := &composetypes.Project{}
	if err := preserveEnvironmentBlueprintResources(candidate, prior, authored, projection.Volumes, true); err != nil {
		t.Fatalf("preserve normalized desired revision: %v", err)
	}
	if candidate.Services["api"].Image != "example/api:1" {
		t.Fatalf("authored Service was not retained: %#v", candidate.Services)
	}
	if _, generated := candidate.Services["router"]; generated {
		t.Fatal("generated Component Service was adopted into authored desired state")
	}
	if candidate.Configs["app-config"].File != "config/app.conf" || candidate.Secrets["app-secret"].File != "secrets/app.secret" {
		t.Fatalf("native Config/Secret state was not retained: configs=%#v secrets=%#v", candidate.Configs, candidate.Secrets)
	}
	if candidate.Volumes["data"].Labels["com.example.owner"] != "operator" {
		t.Fatalf("native Volume state was not retained: %#v", candidate.Volumes["data"])
	}
}

func TestPreserveEnvironmentBlueprintServiceExtensionsUsesPinnedRevisionAuthority(t *testing.T) {
	t.Parallel()
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const dependencyID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FB0"
	extensions, err := preserveEnvironmentBlueprintServiceExtensions(
		map[string]core.ServiceExtensionSpec{},
		map[string]struct{}{},
		[]controller.ComposeResourceIdentity{
			{ID: serviceID, Name: "api"},
			{ID: dependencyID, Name: "migrate"},
		},
		map[string]core.ServiceExtensionSpec{
			"api": {
				Release: &core.ServiceReleaseSpec{
					DefaultStrategy: core.StrategyBlueGreen,
					OnFailure:       core.OnFailureSwitchBack,
				},
				DependsOn: map[string]core.ServiceDependency{
					"migrate": {
						Condition: core.ServiceDependencyCompletedSuccessfully,
						Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy},
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("preserve Environment Blueprint Service extensions: %v", err)
	}
	api := extensions["api"]
	if api.Release == nil || api.Release.DefaultStrategy != core.StrategyBlueGreen ||
		api.Release.OnFailure != core.OnFailureSwitchBack {
		t.Fatalf("preserved release decision = %#v", api.Release)
	}
	dependency := api.DependsOn["migrate"]
	if dependency.Condition != core.ServiceDependencyCompletedSuccessfully ||
		!reflect.DeepEqual(dependency.Phases, []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy}) {
		t.Fatalf("preserved dependency decision = %#v", dependency)
	}
}

func TestSelectRuntimeFilesCarriesOnlyReferencesFromMergedProject(t *testing.T) {
	t.Parallel()
	project := &composetypes.Project{
		Services: composetypes.Services{
			"api": {
				EnvFiles:   []composetypes.EnvFile{{Path: "config/app.env", Required: true}},
				LabelFiles: []string{"config/labels"},
				Volumes:    []composetypes.ServiceVolumeConfig{{Type: composetypes.VolumeTypeBind, Source: "assets"}},
			},
		},
		Configs: composetypes.Configs{"app": {File: "config/app.yaml"}},
	}
	files, err := blueprintparser.SelectRuntimeFiles(
		project,
		[]core.BlueprintFile{{Path: "config/app.env", Content: []byte("NEW=1\n")}},
		[]core.BlueprintFile{
			{Path: "assets/index.html", Content: []byte("index")},
			{Path: "config/app.env", Content: []byte("OLD=1\n")},
			{Path: "config/app.yaml", Content: []byte("server: app\n")},
			{Path: "config/labels", Content: []byte("owner=platform\n")},
			{Path: "obsolete.txt", Content: []byte("obsolete")},
		},
	)
	if err != nil {
		t.Fatalf("select runtime files: %v", err)
	}
	want := []core.BlueprintFile{
		{Path: "assets/index.html", Content: []byte("index")},
		{Path: "config/app.env", Content: []byte("NEW=1\n")},
		{Path: "config/app.yaml", Content: []byte("server: app\n")},
		{Path: "config/labels", Content: []byte("owner=platform\n")},
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("runtime files = %#v, want %#v", files, want)
	}
}

func TestPreserveEnvironmentBlueprintResourcesPreservesDisabledServicesWithoutDuplication(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 21, 30, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 17)
	prior := &composetypes.Project{DisabledServices: composetypes.Services{
		"worker": {Name: "worker", Image: "example/worker:1"},
	}}
	project := &composetypes.Project{}
	if err := preserveEnvironmentBlueprintResources(project, prior, controller.ComposeIdentitySnapshot{
		Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: "worker"}},
	}, nil, true); err != nil {
		t.Fatalf("preserveEnvironmentBlueprintResources() error = %v", err)
	}
	if _, found := project.Services["worker"]; found {
		t.Fatal("profile-disabled retained Service was duplicated into Services")
	}
	if got, found := project.DisabledServices["worker"]; !found || got.Image != "example/worker:1" {
		t.Fatalf("retained DisabledService = %#v, found = %t", got, found)
	}

	project = &composetypes.Project{DisabledServices: composetypes.Services{
		"worker": {Name: "worker", Image: "example/worker:new"},
	}}
	prior = &composetypes.Project{Services: composetypes.Services{
		"worker": {Name: "worker", Image: "example/worker:old"},
	}}
	if err := preserveEnvironmentBlueprintResources(project, prior, controller.ComposeIdentitySnapshot{
		Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: "worker"}},
	}, nil, true); err != nil {
		t.Fatalf("preserveEnvironmentBlueprintResources(existing disabled) error = %v", err)
	}
	if _, found := project.Services["worker"]; found || project.DisabledServices["worker"].Image != "example/worker:new" {
		t.Fatal("existing profile-disabled Service was duplicated or replaced")
	}
}

func TestPreserveEnvironmentBlueprintResourcesRejectsAmbiguousServiceCandidates(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 22, 30, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 19)
	project := &composetypes.Project{}
	prior := &composetypes.Project{Services: composetypes.Services{
		"api__blue":  {Name: "api__blue", Image: "example/api:blue"},
		"api__green": {Name: "api__green", Image: "example/api:green"},
	}}
	err := preserveEnvironmentBlueprintResources(project, prior, controller.ComposeIdentitySnapshot{
		Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: "api"}},
	}, nil, true)
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("ambiguous retained Service error = %v, want resource.in_use", err)
	}
}

func TestPreserveEnvironmentBlueprintResourcesCarriesNativeConfigAndSecretReferences(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 23, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 21)
	prior := &composetypes.Project{
		Services: composetypes.Services{"api": {Name: "api", Image: "example/api:1"}},
		Configs:  composetypes.Configs{"app-config": {Name: "app-config", File: "config/app.conf"}},
		Secrets:  composetypes.Secrets{"app-secret": {Name: "app-secret", File: "secrets/app.secret"}},
	}
	project := &composetypes.Project{}
	if err := preserveEnvironmentBlueprintResources(project, prior, controller.ComposeIdentitySnapshot{
		Services: []controller.ComposeResourceIdentity{{ID: serviceID, Name: "api"}},
	}, nil, true); err != nil {
		t.Fatalf("preserveEnvironmentBlueprintResources(configs and secrets) error = %v", err)
	}
	if config, found := project.Configs["app-config"]; !found || config.File != "config/app.conf" {
		t.Fatalf("retained Config = %#v, found = %t", config, found)
	}
	if secret, found := project.Secrets["app-secret"]; !found || secret.File != "secrets/app.secret" {
		t.Fatalf("retained Secret = %#v, found = %t", secret, found)
	}
}

func TestPreserveEnvironmentBlueprintResourcesCarriesNativeVolumeConfig(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 23, 30, 0, 0, time.UTC)
	volumeID := ids.NewAt(ids.KindVolume, at, 23)
	priorVolume := composetypes.VolumeConfig{
		Name:       "authored-data",
		Driver:     "local",
		DriverOpts: composetypes.Options{"type": "none", "o": "bind", "device": "./data"},
		Labels:     composetypes.Labels{"com.example.owner": "operator"},
		Extensions: composetypes.Extensions{"x-custom": "keep"},
	}
	prior := &composetypes.Project{Volumes: composetypes.Volumes{"data": priorVolume}}
	project := &composetypes.Project{}

	err := preserveEnvironmentBlueprintResources(project, prior, controller.ComposeIdentitySnapshot{},
		[]etcd.EnvironmentVolumeIdentity{{ID: volumeID, Key: "data", Slug: "stable-data"}}, true)
	if err != nil {
		t.Fatalf("preserve omitted volume: %v", err)
	}

	got, found := project.Volumes["data"]
	if !found {
		t.Fatal("preserved volume missing")
	}
	if got.Name != priorVolume.Name || got.Driver != priorVolume.Driver {
		t.Fatalf("preserved volume identity = %#v, want %#v", got, priorVolume)
	}
	if !reflect.DeepEqual(got.DriverOpts, priorVolume.DriverOpts) {
		t.Fatalf("preserved volume driver options = %#v, want %#v", got.DriverOpts, priorVolume.DriverOpts)
	}
	if !reflect.DeepEqual(got.Labels, priorVolume.Labels) {
		t.Fatalf("preserved volume labels = %#v, want %#v", got.Labels, priorVolume.Labels)
	}
	if got.Extensions["x-custom"] != "keep" || got.Extensions["x-gp-slug"] != "stable-data" {
		t.Fatalf("preserved volume extensions = %#v", got.Extensions)
	}
}

func TestPreserveEnvironmentBlueprintRoutesCarriesForwardOmittedRoute(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 22, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 13)
	serviceID := ids.NewAt(ids.KindService, at, 14)
	routeID := ids.NewAt(ids.KindRoute, at, 15)
	record, err := etcd.NewRouteRecord(environmentID, core.Route{
		ID: routeID, Host: "old.example.com", Path: "/legacy/*", TargetServiceID: serviceID,
		TargetPort: 8080, Exposure: "internal",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	specs, err := preserveEnvironmentBlueprintRoutes(nil, []core.Service{{ID: serviceID, Name: "api"}}, []etcd.Versioned[etcd.RouteRecord]{{Record: record}})
	if err != nil {
		t.Fatalf("preserveEnvironmentBlueprintRoutes() error = %v", err)
	}
	if len(specs) != 1 || specs[0].Hostname != record.Desired.Host || specs[0].Path != record.Desired.Path ||
		specs[0].Target != "api" || specs[0].TargetPort != record.Desired.TargetPort || specs[0].Exposure != record.Desired.Exposure {
		t.Fatalf("retained Route specs = %#v", specs)
	}
}
