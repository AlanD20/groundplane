package composerender

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

// Rationale: a portless recreate workload is still a logical replicated Service, so its canonical Compose artifact
// and Agent observation metadata must preserve the exact operator-authored count.
func TestRenderComposePortlessReplicaCountPreserved(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 101)
	releaseID := composeIdentityTestID(ids.KindDeployment, 102)
	replicas := 3
	input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{
		"worker": {Image: "example/worker:previous", Deploy: &composetypes.DeployConfig{Replicas: &replicas}},
	}})
	input.Identities.Services = []composeidentity.Resource{{ID: serviceID, Name: "worker"}}
	input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
		ReleaseID: releaseID, Image: "example/worker:next", Strategy: domain.StrategyRecreate,
		Target: domain.WorkloadSingleton, ServingTarget: domain.WorkloadSingleton,
		ServingReleaseID: releaseID, ServingProxyGeneration: 1,
	}}

	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("RenderCompose() error = %v", err)
	}
	if len(artifact.Services) != 1 || artifact.Services[0].ComposeName != "worker" ||
		artifact.Services[0].ExpectedReplicas != 3 {
		t.Fatalf("portless recreate metadata = %#v, want worker with three replicas", artifact.Services)
	}
	var rendered struct {
		Services map[string]struct {
			Deploy struct {
				Replicas int `yaml:"replicas"`
			} `yaml:"deploy"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &rendered); err != nil {
		t.Fatalf("decode rendered Compose YAML: %v", err)
	}
	if rendered.Services["worker"].Deploy.Replicas != 3 {
		t.Fatalf("portless recreate YAML replicas = %d, want 3", rendered.Services["worker"].Deploy.Replicas)
	}
}

// Rationale: the MVP blue-green topology has two physical singleton slots and cannot safely represent an authored
// replica count greater than one.
func TestRenderComposeRejectsBlueGreenReplicaCountAboveOne(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 105)
	releaseID := composeIdentityTestID(ids.KindDeployment, 106)
	replicas := 3
	input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{
		"api": {
			Image: "example/api:previous", Expose: []string{"8080"},
			Deploy: &composetypes.DeployConfig{Replicas: &replicas},
		},
	}})
	input.Identities.Services = []composeidentity.Resource{{ID: serviceID, Name: "api"}}
	input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
		ReleaseID: releaseID, Image: "example/api:next", Strategy: domain.StrategyBlueGreen,
		Target: domain.WorkloadGreen, ServingTarget: domain.WorkloadBlue,
		ServingReleaseID: composeIdentityTestID(ids.KindDeployment, 107), ServingProxyGeneration: 1,
	}}

	_, err := RenderCompose(input)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("RenderCompose() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}

// Rationale: direct renderer callers must fail closed on explicit zero and negative native Compose replica counts,
// even if an earlier Blueprint validation boundary would normally reject them.
func TestRenderComposeRejectsInvalidReplicaCount(t *testing.T) {
	for _, test := range []struct {
		name     string
		replicas int
		expose   []string
	}{
		{name: "portless zero", replicas: 0},
		{name: "portless negative", replicas: -1},
		{name: "addressable zero", replicas: 0, expose: []string{"8080"}},
		{name: "addressable negative", replicas: -1, expose: []string{"8080"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			serviceID := composeIdentityTestID(ids.KindService, 108)
			releaseID := composeIdentityTestID(ids.KindDeployment, 109)
			input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{
				"worker": {
					Image: "example/worker:previous", Expose: test.expose,
					Deploy: &composetypes.DeployConfig{Replicas: &test.replicas},
				},
			}})
			input.Identities.Services = []composeidentity.Resource{{ID: serviceID, Name: "worker"}}
			input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
				ProxyImage: testServiceProxyImage(),
				ReleaseID:  releaseID, Image: "example/worker:next", Strategy: domain.StrategyRecreate,
				Target: domain.WorkloadSingleton, ServingTarget: domain.WorkloadSingleton,
				ServingReleaseID: releaseID, ServingProxyGeneration: 1,
			}}

			_, err := RenderCompose(input)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("RenderCompose() error = %v, want %q", err, errs.CodeValidationFailed)
			}
		})
	}
}

// Rationale: omitting native Compose deploy.replicas means one instance; preserving that Compose default must not
// require a compatibility record or materialize a synthetic authored count.
func TestRenderComposeReplicaDefaultWhenOmitted(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 110)
	releaseID := composeIdentityTestID(ids.KindDeployment, 111)
	input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{
		"worker": {Image: "example/worker:previous"},
	}})
	input.Identities.Services = []composeidentity.Resource{{ID: serviceID, Name: "worker"}}
	input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
		ReleaseID: releaseID, Image: "example/worker:next", Strategy: domain.StrategyRecreate,
		Target: domain.WorkloadSingleton, ServingTarget: domain.WorkloadSingleton,
		ServingReleaseID: releaseID, ServingProxyGeneration: 1,
	}}

	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("RenderCompose() error = %v", err)
	}
	if len(artifact.Services) != 1 || artifact.Services[0].ExpectedReplicas != 1 {
		t.Fatalf("omitted replica metadata = %#v, want Compose default of one", artifact.Services)
	}
	var rendered struct {
		Services map[string]struct {
			Deploy *struct {
				Replicas *int `yaml:"replicas"`
			} `yaml:"deploy"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &rendered); err != nil {
		t.Fatalf("decode rendered Compose YAML: %v", err)
	}
	if rendered.Services["worker"].Deploy != nil {
		t.Fatalf("omitted replica YAML materialized deploy = %#v", rendered.Services["worker"].Deploy)
	}
}

// Rationale: stable routing is one proxy per logical addressable Service while the recreate workload retains its
// independently authored physical replica count.
func TestRenderComposeAddressableReplicaCountKeepsSingletonProxy(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 103)
	releaseID := composeIdentityTestID(ids.KindDeployment, 104)
	replicas := 3
	input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{
		"api": {
			Image: "example/api:previous", Expose: []string{"8080"},
			Deploy: &composetypes.DeployConfig{Replicas: &replicas},
		},
	}})
	input.Identities.Services = []composeidentity.Resource{{ID: serviceID, Name: "api"}}
	input.Releases = map[string]ComposeReleaseIdentity{serviceID: {
		ReleaseID: releaseID, Image: "example/api:next", Strategy: domain.StrategyRecreate,
		ProxyImage: testServiceProxyImage(),
		Target:     domain.WorkloadSingleton, ServingTarget: domain.WorkloadSingleton,
		ServingReleaseID: releaseID, ServingProxyGeneration: 1,
	}}

	artifact, err := RenderCompose(input)
	if err != nil {
		t.Fatalf("RenderCompose() error = %v", err)
	}
	metadata := make(map[string]*agentpb.ComposeService, len(artifact.Services))
	for _, service := range artifact.Services {
		metadata[service.ComposeName] = service
	}
	if len(metadata) != 2 || metadata["api"] == nil || metadata["api"].ExpectedReplicas != 1 ||
		metadata["api"].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
		t.Fatalf("stable proxy metadata = %#v, want one stable proxy", metadata["api"])
	}
	workload := metadata["api--singleton"]
	if workload == nil || workload.ExpectedReplicas != 3 ||
		workload.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
		t.Fatalf("recreate workload metadata = %#v, want three logical workload replicas", workload)
	}
	var rendered struct {
		Services map[string]struct {
			Deploy struct {
				Replicas int `yaml:"replicas"`
			} `yaml:"deploy"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &rendered); err != nil {
		t.Fatalf("decode rendered Compose YAML: %v", err)
	}
	if len(rendered.Services) != 2 || rendered.Services["api--singleton"].Deploy.Replicas != 3 {
		t.Fatalf(
			"addressable recreate YAML services = %#v, want singleton workload with three replicas",
			rendered.Services,
		)
	}
}
