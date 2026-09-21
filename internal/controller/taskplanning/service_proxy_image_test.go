package taskplanning

import (
	"crypto/sha256"
	"strings"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

// Rationale: the Agent refuses to start managed stable proxies without the
// compiled index, selected platform manifest and image/config identity.
func TestRenderComposeStableProxySealsManagedImageAuthority(t *testing.T) {
	serviceID := composeIdentityTestID(ids.KindService, 33)
	input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{"api": {
		Image: "example/api:previous", Expose: []string{"8080"},
	}}})
	input.Identities.Services = []testcomposeidentity.Resource{{ID: serviceID, Name: "api"}}
	input.Releases = map[string]testcomposerender.ComposeReleaseIdentity{serviceID: {
		ProxyImage: testServiceProxyImage(),
		ReleaseID:  composeIdentityTestID(ids.KindDeployment, 34), Image: "example/api:next",
		Strategy: domain.StrategyRecreate, Target: domain.WorkloadSingleton,
		ServingTarget: domain.WorkloadSingleton,
	}}
	artifact, err := testcomposerender.RenderCompose(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range artifact.Services {
		if service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if service.ImageReference == "" || service.ImageRepository == "" || service.ImageOs != "linux" ||
			service.ImageArchitecture == "" || len(service.ImageIndexDigest) != sha256.Size ||
			len(service.ImageChildDigest) != sha256.Size || len(service.ImageConfigDigest) != sha256.Size {
			t.Fatal("stable proxy lacks sealed managed image authority")
		}
		var rendered struct {
			Services map[string]struct {
				Image  string            `yaml:"image"`
				Labels map[string]string `yaml:"labels"`
			} `yaml:"services"`
		}
		if err := yaml.Unmarshal(artifact.CanonicalYaml, &rendered); err != nil {
			t.Fatal(err)
		}
		native := rendered.Services["api"]
		if native.Image != testServiceProxyImage().Reference() || native.Image != service.ImageReference ||
			native.Labels["com.groundplane.image-config-digest"] != "sha256:"+testServiceProxyImage().Platform.ConfigDigest {
			t.Fatal("native proxy image differs from sealed image metadata")
		}
		return
	}
	t.Fatal("stable proxy missing")
}

// Rationale: replay must use the historical proxy, even after a Controller upgrade
// changes its compiled asset. Missing historical authority must never fall back.
func TestPrepareReleaseProxyImageRetainsHistoricalAuthority(t *testing.T) {
	current := testServiceProxyImage()
	current.Platform.ConfigDigest = strings.Repeat("d", 64)
	resolver := &TaskPlanResolver{serviceProxyImage: current}
	old := testServiceProxyImage()
	prior := testreleaserender.ReleaseRenderInput{ServiceID: "service", EnvironmentID: "environment", ProxyImage: old}
	for _, test := range []struct {
		name      string
		prior     *testreleaserender.ReleaseRenderInput
		image     *domain.ProxyImage
		wantError bool
	}{
		{name: "historical read", prior: &prior},
		{name: "already frozen", image: old},
		{name: "missing historical", wantError: true},
		{name: "partial historical", prior: &testreleaserender.ReleaseRenderInput{ServiceID: "service", EnvironmentID: "environment"}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			render := testreleaserender.ReleaseRenderInput{ServiceID: "service", EnvironmentID: "environment",
				ProxyPorts: []uint16{8080}, PriorArtifactID: "prior-artifact", ProxyImage: test.image}
			err := resolver.PrepareReleaseProxyImage(&render, test.prior)
			if (err != nil) != test.wantError {
				t.Fatalf("prepare error = %v", err)
			}
			if !test.wantError && (render.ProxyImage == old || *render.ProxyImage != *old) {
				t.Fatal("historical proxy was not retained as an independent frozen value")
			}
		})
	}
}

// Rationale: only the first addressable release selects the compiled asset;
// a portless release neither needs nor acquires proxy authority.
func TestPrepareReleaseProxyImageFirstSelection(t *testing.T) {
	resolver := &TaskPlanResolver{serviceProxyImage: testServiceProxyImage()}
	render := testreleaserender.ReleaseRenderInput{ProxyPorts: []uint16{8080}}
	if err := resolver.PrepareReleaseProxyImage(&render, nil); err != nil {
		t.Fatal(err)
	}
	if render.ProxyImage == resolver.serviceProxyImage || *render.ProxyImage != *resolver.serviceProxyImage {
		t.Fatal("first proxy selection is not independently frozen")
	}
	portless := testreleaserender.ReleaseRenderInput{}
	if err := resolver.PrepareReleaseProxyImage(&portless, nil); err != nil || portless.ProxyImage != nil {
		t.Fatalf("portless selection = %#v, error = %v", portless.ProxyImage, err)
	}
	if err := (&TaskPlanResolver{}).PrepareReleaseProxyImage(&testreleaserender.ReleaseRenderInput{ProxyPorts: []uint16{8080}}, nil); err == nil {
		t.Fatal("accepted missing compiled proxy authority")
	}
}

func testServiceProxyImage() *domain.ProxyImage {
	return &domain.ProxyImage{Repository: "docker.io/library/caddy", IndexDigest: strings.Repeat("a", 64),
		Platform: componentsdk.OCIPlatform{OS: "linux", Architecture: "amd64",
			ChildDigest: strings.Repeat("b", 64), ConfigDigest: strings.Repeat("c", 64)}}
}
