package controller

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

func TestRenderComposePortlessRecreateGroupMemberIsSingleton(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 31)
	project := &composetypes.Project{Services: composetypes.Services{"worker": composetypes.ServiceConfig{Image: "example/worker:previous"}}}
	input := composeRenderTestInput(project)
	input.Identities.Services = []ComposeResourceIdentity{{ID: serviceID, Name: "worker"}}
	input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
		ReleaseID: composeIdentityTestID(ids.KindDeployment, 32), Image: "example/worker:next", Strategy: domain.StrategyRecreate,
		Target: domain.WorkloadSingleton, ServingTarget: domain.WorkloadSingleton,
		ServingReleaseID: "baseline", ServingProxyGeneration: 1,
	}}
	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("RenderCompose() error = %v", err)
	}
	if len(artifact.Services) != 1 || artifact.Services[0].ComposeName != "worker" ||
		artifact.Services[0].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
		t.Fatalf("portless recreate services = %#v, want one singleton", artifact.Services)
	}
	labels := labelPairMap(artifact.Services[0].ExpectedLabels)
	if labels[composeLabelRuntimeRole] != "singleton" || labels[composeLabelReleaseID] != input.Releases[serviceID].ReleaseID {
		t.Fatalf("portless recreate labels = %#v", labels)
	}
}

func TestRenderComposeAddressableRecreateHasOneWorkloadAndStableProxy(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 33)
	project := &composetypes.Project{Services: composetypes.Services{"api": composetypes.ServiceConfig{Image: "example/api:previous", Expose: []string{"8080"}}}}
	input := composeRenderTestInput(project)
	input.Identities.Services = []ComposeResourceIdentity{{ID: serviceID, Name: "api"}}
	input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
		ReleaseID: composeIdentityTestID(ids.KindDeployment, 34), Image: "example/api:next", Strategy: domain.StrategyRecreate,
		Target: domain.WorkloadSingleton, ServingTarget: domain.WorkloadBlue,
		ServingReleaseID: "baseline", ServingProxyGeneration: 1,
	}}
	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("RenderCompose() error = %v", err)
	}
	roles, candidate := map[agentpb.ComposeServiceRole]int{}, false
	for _, service := range artifact.Services {
		roles[service.Role]++
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			candidate = labelPairMap(service.ExpectedLabels)[composeLabelReleaseID] == input.Releases[serviceID].ReleaseID
		}
	}
	if roles[agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY] != 1 ||
		roles[agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON] != 1 ||
		roles[agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT] != 0 || !candidate {
		t.Fatalf("addressable recreate roles = %#v candidate=%v", roles, candidate)
	}
}

// Rationale: rendering must preserve native Compose fields and profile-disabled services while adding only generated
// identity and ownership data.
func TestRenderComposeBuildsCanonicalOwnedArtifact(t *testing.T) {
	apiID := composeIdentityTestID(ids.KindService, 21)
	workerID := composeIdentityTestID(ids.KindService, 22)
	networkID := composeIdentityTestID(ids.KindNetwork, 23)
	externalNetworkID := composeIdentityTestID(ids.KindNetwork, 27)
	volumeID := composeIdentityTestID(ids.KindVolume, 28)
	replicas := 2
	project := &composetypes.Project{
		Name: "authored-name",
		Services: composetypes.Services{
			"api": composetypes.ServiceConfig{
				Image:       "example/api:1",
				Labels:      composetypes.Labels{"example.owner": "operator"},
				HealthCheck: &composetypes.HealthCheckConfig{},
			},
		},
		DisabledServices: composetypes.Services{
			"worker": composetypes.ServiceConfig{
				Image:    "example/worker:1",
				Profiles: []string{"batch"},
				Deploy:   &composetypes.DeployConfig{Replicas: &replicas},
			},
		},
		Networks: composetypes.Networks{
			"frontend": composetypes.NetworkConfig{Internal: true},
			"shared":   composetypes.NetworkConfig{External: true, Name: "operator-supplied"},
		},
		Volumes: composetypes.Volumes{
			"data": composetypes.VolumeConfig{Labels: composetypes.Labels{"example.storage": "persistent"}},
		},
	}
	input := composeRenderTestInput(project)
	input.Identities = ComposeIdentitySnapshot{
		Services: []ComposeResourceIdentity{{ID: apiID, Name: "api"}, {ID: workerID, Name: "worker"}},
		Networks: []ComposeResourceIdentity{{ID: networkID, Name: "frontend"}},
		Volumes:  []ComposeResourceIdentity{{ID: volumeID, Name: "data"}},
	}
	input.ExternalNetworks = []ComposeResourceIdentity{{ID: externalNetworkID, Name: "shared"}}

	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("RenderCompose() error = %v", err)
	}
	if artifact.ProjectName != "gp-"+composeRenderTestEnvironmentIDLower {
		t.Fatalf("project name = %q, want stable environment-derived name", artifact.ProjectName)
	}
	if artifact.OwnerId != composeRenderTestEnvironmentID || artifact.ArtifactId != composeRenderTestArtifactID {
		t.Fatalf("artifact identity = (%q, %q), want environment and config ids", artifact.OwnerId, artifact.ArtifactId)
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if !reflect.DeepEqual(artifact.YamlSha256, digest[:]) {
		t.Fatal("artifact YAML digest does not match canonical YAML")
	}
	if len(artifact.Services) != 2 || artifact.Services[0].ServiceId > artifact.Services[1].ServiceId {
		t.Fatalf("services are not complete and sorted by stable id: %#v", artifact.Services)
	}
	services := make(map[string]renderedServiceProjection, len(artifact.Services))
	for _, service := range artifact.Services {
		services[service.ComposeName] = renderedServiceProjection{
			ID:               service.ServiceId,
			ExpectedReplicas: service.ExpectedReplicas,
			HasHealthcheck:   service.HasHealthcheck,
			Labels:           labelPairMap(service.ExpectedLabels),
		}
	}
	if services["api"].ID != apiID || services["api"].ExpectedReplicas != 1 || !services["api"].HasHealthcheck {
		t.Fatalf("api artifact projection = %#v", services["api"])
	}
	if services["worker"].ID != workerID || services["worker"].ExpectedReplicas != 2 {
		t.Fatalf("worker artifact projection = %#v", services["worker"])
	}
	assertSortedLabelPairs(t, artifact.Services[0].ExpectedLabels)
	if len(artifact.Networks) != 1 || artifact.Networks[0].DockerName != "gp_net_"+networkID {
		t.Fatalf("network artifact projection = %#v", artifact.Networks)
	}
	if len(artifact.Volumes) != 1 || artifact.Volumes[0].DockerName != "gp_vol_"+strings.ToLower(volumeID) {
		t.Fatalf("volume artifact projection = %#v", artifact.Volumes)
	}

	var document renderedComposeDocument
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if document.Name != artifact.ProjectName || document.Services["api"].Image != "example/api:1" {
		t.Fatalf("rendered document lost project or native service fields: %#v", document)
	}
	if !reflect.DeepEqual(document.Services["worker"].Profiles, []string{"batch"}) {
		t.Fatalf("worker profiles = %#v, want batch profile preserved", document.Services["worker"].Profiles)
	}
	if document.Services["api"].Labels["example.owner"] != "operator" {
		t.Fatalf("operator label was not preserved: %#v", document.Services["api"].Labels)
	}
	if document.Services["api"].Resource.ID != apiID || document.Services["api"].Resource.Kind != "service" {
		t.Fatalf("generated service resource metadata = %#v", document.Services["api"].Resource)
	}
	if document.Networks["frontend"].Name != "gp_net_"+networkID || !document.Networks["frontend"].Internal {
		t.Fatalf("rendered network = %#v", document.Networks["frontend"])
	}
	if document.Networks["shared"].Name != "gp_net_"+externalNetworkID || !document.Networks["shared"].External {
		t.Fatalf("rendered external network = %#v", document.Networks["shared"])
	}
	if len(document.Networks["shared"].Labels) != 0 || document.Networks["shared"].Resource.ID != "" {
		t.Fatalf("consumer claimed ownership of external network: %#v", document.Networks["shared"])
	}
	volume := document.Volumes["data"]
	if volume.Name != "gp_vol_"+strings.ToLower(volumeID) || volume.Driver != "local" ||
		volume.DriverOpts["type"] != "none" || volume.DriverOpts["o"] != "bind" ||
		volume.DriverOpts["device"] != input.AuthorizedVolumeDir+"/data" ||
		volume.Labels["example.storage"] != "persistent" || volume.Resource.ID != volumeID {
		t.Fatalf("rendered managed volume = %#v", volume)
	}
	if project.Name != "authored-name" || len(project.DisabledServices) != 1 ||
		len(project.Services["api"].Labels) != 1 {
		t.Fatal("RenderCompose() mutated its compose-go input")
	}
}

// Rationale: the same desired project and durable records must produce byte-identical canonical YAML and metadata.
func TestRenderComposeIsDeterministic(t *testing.T) {
	project := &composetypes.Project{Services: composetypes.Services{
		"worker": composetypes.ServiceConfig{Image: "example/worker:1"},
		"api":    composetypes.ServiceConfig{Image: "example/api:1"},
	}}
	input := composeRenderTestInput(project)
	input.Identities = ComposeIdentitySnapshot{Services: []ComposeResourceIdentity{
		{ID: composeIdentityTestID(ids.KindService, 24), Name: "api"},
		{ID: composeIdentityTestID(ids.KindService, 25), Name: "worker"},
	}}

	first, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("first RenderCompose() error = %v", err)
	}
	second, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("second RenderCompose() error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("RenderCompose() returned different artifacts for identical input")
	}
}

// Rationale: renderer input comes from durable Controller records, so incomplete identity coverage is corruption.
func TestRenderComposeRejectsIdentityCoverageMismatch(t *testing.T) {
	project := &composetypes.Project{Services: composetypes.Services{"api": composetypes.ServiceConfig{}}}
	_, err := RenderCompose(composeRenderTestInput(project))
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("RenderCompose() error = %v, want %q", err, errs.CodeInternal)
	}
}

// Rationale: operator-authored labels cannot occupy the Controller's closed ownership namespace.
func TestRenderComposeRejectsAuthoredOwnershipLabel(t *testing.T) {
	project := &composetypes.Project{Services: composetypes.Services{
		"api": composetypes.ServiceConfig{Labels: composetypes.Labels{"com.groundplane.managed": "false"}},
	}}
	input := composeRenderTestInput(project)
	input.Identities.Services = []ComposeResourceIdentity{
		{ID: composeIdentityTestID(ids.KindService, 26), Name: "api"},
	}

	_, err := RenderCompose(input)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("RenderCompose() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}

// Rationale: every external network must arrive with the durable owner id resolved before rendering.
func TestRenderComposeRejectsUnresolvedExternalNetwork(t *testing.T) {
	project := &composetypes.Project{
		Networks: composetypes.Networks{"shared": composetypes.NetworkConfig{External: true}},
	}

	_, err := RenderCompose(composeRenderTestInput(project))
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("RenderCompose() error = %v, want %q", err, errs.CodeInternal)
	}
}

// Rationale: unresolved network ownership extensions fail visibly instead of being silently ignored.
func TestRenderComposeRejectsUndefinedResourceContracts(t *testing.T) {
	cases := map[string]*composetypes.Project{
		"network ownership extension": {
			Networks: composetypes.Networks{"shared": composetypes.NetworkConfig{
				Extensions: composetypes.Extensions{"x-gp-network": map[string]string{"id": "net_pending"}},
			}},
		},
	}
	for name, project := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := RenderCompose(composeRenderTestInput(project))
			if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
				t.Fatalf("RenderCompose() error = %v, want %q", err, errs.CodeNotImplemented)
			}
		})
	}
}

// Rationale: external or custom-driver volumes need a separate resolved ownership/materialization contract and must
// not be rewritten as Groundplane-managed local data.
func TestRenderComposeRejectsUnresolvedVolumeRuntime(t *testing.T) {
	cases := map[string]composetypes.VolumeConfig{
		"external":    {External: true},
		"driver opts": {Driver: "local", DriverOpts: composetypes.Options{"type": "none", "o": "bind"}},
		"custom driver": {
			Driver: "custom",
		},
	}
	for name, volume := range cases {
		t.Run(name, func(t *testing.T) {
			project := &composetypes.Project{Volumes: composetypes.Volumes{"data": volume}}
			input := composeRenderTestInput(project)
			input.Identities.Volumes = []ComposeResourceIdentity{
				{ID: composeIdentityTestID(ids.KindVolume, 29), Name: "data"},
			}
			_, err := RenderCompose(input)
			if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
				t.Fatalf("RenderCompose() error = %v, want %q", err, errs.CodeNotImplemented)
			}
		})
	}
}

const (
	composeRenderTestEnvironmentID      = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	composeRenderTestEnvironmentIDLower = "env_01arz3ndektsv4rrffq69g5fav"
	composeRenderTestArtifactID         = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func composeRenderTestInput(project *composetypes.Project) ComposeRenderInput {
	return ComposeRenderInput{
		Project:          project,
		ArtifactID:       composeRenderTestArtifactID,
		TenantID:         "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ProjectID:        "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		EnvironmentID:    composeRenderTestEnvironmentID,
		PlanID:           "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RenderGeneration: 7,
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + composeRenderTestEnvironmentID,
	}
}

type renderedComposeDocument struct {
	Name     string                            `yaml:"name"`
	Services map[string]renderedComposeService `yaml:"services"`
	Networks map[string]renderedComposeNetwork `yaml:"networks"`
	Volumes  map[string]renderedComposeVolume  `yaml:"volumes"`
}

type renderedComposeService struct {
	Image    string                  `yaml:"image"`
	Profiles []string                `yaml:"profiles"`
	Labels   map[string]string       `yaml:"labels"`
	Resource renderedComposeResource `yaml:"x-gp-resource"`
}

type renderedComposeNetwork struct {
	Name     string                  `yaml:"name"`
	Internal bool                    `yaml:"internal"`
	External bool                    `yaml:"external"`
	Labels   map[string]string       `yaml:"labels"`
	Resource renderedComposeResource `yaml:"x-gp-resource"`
}

type renderedComposeVolume struct {
	Name       string                  `yaml:"name"`
	Driver     string                  `yaml:"driver"`
	DriverOpts map[string]string       `yaml:"driver_opts"`
	Labels     map[string]string       `yaml:"labels"`
	Resource   renderedComposeResource `yaml:"x-gp-resource"`
}

type renderedComposeResource struct {
	Kind   string `yaml:"kind"`
	ID     string `yaml:"id"`
	Parent struct {
		EnvironmentID string `yaml:"environment_id"`
	} `yaml:"parent"`
}

type renderedServiceProjection struct {
	ID               string
	ExpectedReplicas uint32
	HasHealthcheck   bool
	Labels           map[string]string
}

func labelPairMap(pairs []*agentpb.LabelPair) map[string]string {
	labels := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		labels[pair.Key] = pair.Value
	}
	return labels
}

func assertSortedLabelPairs(t *testing.T, pairs []*agentpb.LabelPair) {
	t.Helper()
	previous := ""
	for _, pair := range pairs {
		if pair.Key <= previous {
			t.Fatalf("label pairs are not sorted: %#v", pairs)
		}
		previous = pair.Key
	}
}
