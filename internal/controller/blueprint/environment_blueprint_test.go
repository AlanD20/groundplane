package blueprint

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

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
	record, err := testservices.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "api", Image: "app:old",
	}, "")
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	record, err = testservices.SetServiceRuntimeIntent(record, core.ServiceRuntimeIntentStopped)
	if err != nil {
		t.Fatalf("SetServiceRuntimeIntent() error = %v", err)
	}
	current := testkeyvalue.Versioned[testservices.ServiceRecord]{Record: record, Revision: 7, ReadRevision: 9}

	changes, err := blueprintrelease.PrepareServiceChanges(
		environmentID,
		[]core.Service{{ID: serviceID, Name: "api", Image: "app:new"}},
		[]testkeyvalue.Versioned[testservices.ServiceRecord]{current},
	)
	if err != nil {
		t.Fatalf("PrepareServiceChanges() error = %v", err)
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

	changes, err := blueprintrelease.PrepareServiceChanges(
		environmentID,
		[]core.Service{{ID: serviceID, Name: "worker", Image: "app:1"}},
		nil,
	)
	if err != nil {
		t.Fatalf("PrepareServiceChanges() error = %v", err)
	}
	if len(changes) != 1 || changes[0].Current != nil ||
		changes[0].Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("changes = %#v", changes)
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
	err := preserveEnvironmentBlueprintResources(project, prior, testcomposeidentity.Snapshot{
		Services: []testcomposeidentity.Resource{{ID: serviceID, Name: "api"}},
		Networks: []testcomposeidentity.Resource{{ID: networkID, Name: "backend"}},
	}, []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Key: "data", Slug: "data"}}, true)
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
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, ComposeArtifact: artifact, NormalizedCompose: authoredCanonical,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: core.Service{ID: serviceID, Name: "api"},
		}},
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{
			{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Key: "data", Slug: "data"},
		},
	}
	desired := testenvironmentprojection.EnvironmentDesiredInput{
		EnvironmentID: environmentID, RevisionID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Input: core.BlueprintDesiredInput{NormalizedCompose: authoredCanonical},
	}
	owned := testenvironmentprojection.EnvironmentOwnedIdentities{
		EnvironmentID: environmentID, RevisionID: desired.RevisionID, RenderGeneration: 1,
		Services: []testenvironmentprojection.OwnedIdentity{{ID: serviceID, Name: "api"}},
		Volumes:  []testenvironmentprojection.OwnedIdentity{{ID: projection.Volumes[0].ID, Name: "data", Slug: "data"}},
	}
	authored, err := authoredOwnedIdentitySnapshot(context.Background(), desired, owned)
	if err != nil {
		t.Fatalf("load authored identity snapshot: %v", err)
	}
	if !reflect.DeepEqual(authored.Services, []testcomposeidentity.Resource{{ID: serviceID, Name: "api"}}) {
		t.Fatalf("authored Service identities = %#v", authored.Services)
	}
	prior, err := testcomposerender.LoadNormalizedEnvironmentProject(context.Background(), projection)
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
	if len(candidate.Configs) != 0 || len(candidate.Secrets) != 0 {
		t.Fatalf(
			"omitted native Config/Secret state was retained: configs=%#v secrets=%#v",
			candidate.Configs,
			candidate.Secrets,
		)
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
		[]testcomposeidentity.Resource{
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
	if err := preserveEnvironmentBlueprintResources(project, prior, testcomposeidentity.Snapshot{
		Services: []testcomposeidentity.Resource{{ID: serviceID, Name: "worker"}},
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
	if err := preserveEnvironmentBlueprintResources(project, prior, testcomposeidentity.Snapshot{
		Services: []testcomposeidentity.Resource{{ID: serviceID, Name: "worker"}},
	}, nil, true); err != nil {
		t.Fatalf("preserveEnvironmentBlueprintResources(existing disabled) error = %v", err)
	}
	if _, found := project.Services["worker"]; found ||
		project.DisabledServices["worker"].Image != "example/worker:new" {
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
	err := preserveEnvironmentBlueprintResources(project, prior, testcomposeidentity.Snapshot{
		Services: []testcomposeidentity.Resource{{ID: serviceID, Name: "api"}},
	}, nil, true)
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("ambiguous retained Service error = %v, want resource.in_use", err)
	}
}

func TestPreserveEnvironmentBlueprintResourcesOmitsNativeConfigAndSecretReferences(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 22, 23, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 21)
	prior := &composetypes.Project{
		Services: composetypes.Services{"api": {Name: "api", Image: "example/api:1"}},
		Configs:  composetypes.Configs{"app-config": {Name: "app-config", File: "config/app.conf"}},
		Secrets:  composetypes.Secrets{"app-secret": {Name: "app-secret", File: "secrets/app.secret"}},
	}
	project := &composetypes.Project{}
	if err := preserveEnvironmentBlueprintResources(project, prior, testcomposeidentity.Snapshot{
		Services: []testcomposeidentity.Resource{{ID: serviceID, Name: "api"}},
	}, nil, true); err != nil {
		t.Fatalf("preserveEnvironmentBlueprintResources(configs and secrets) error = %v", err)
	}
	if len(project.Configs) != 0 || len(project.Secrets) != 0 {
		t.Fatalf("omitted Config/Secret state was retained: configs=%#v secrets=%#v", project.Configs, project.Secrets)
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

	err := preserveEnvironmentBlueprintResources(
		project,
		prior,
		testcomposeidentity.Snapshot{},
		[]testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Key: "data", Slug: "stable-data"}},
		true,
	)
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
