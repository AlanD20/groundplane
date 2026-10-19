package taskplanning

import (
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	compose "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Native render data is independent of the retired TaskRecord/plan resolver.
// The actual renderer still seals workload, proxy and resource ownership.
func nativeRetainedRenderFixture(t *testing.T, strategy domain.Strategy) releaserender.ReleaseRenderInput {
	t.Helper()
	project := &compose.Project{Services: compose.Services{"api": {Name: "api", Image: "example/api:next",
		Expose: []string{"8080"}, Networks: map[string]*compose.ServiceNetworkConfig{"frontend": {}},
		HealthCheck: &compose.HealthCheckConfig{Test: compose.HealthCheckTest{"CMD", "true"}}}},
		Networks: compose.Networks{"frontend": {}}, Volumes: compose.Volumes{"app-data": {}}}
	base := composeRenderTestInput(project)
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	projection := environmentprojection.EnvironmentComposeProjection{EnvironmentID: base.EnvironmentID,
		RevisionID: ids.New(ids.KindTask), RenderGeneration: base.RenderGeneration, NormalizedCompose: normalized,
		DesiredServices: []services.EnvironmentServiceProjection{{EnvironmentID: base.EnvironmentID,
			Desired: core.Service{ID: serviceID, Name: "api", Image: "example/api:next", Zones: []string{"frontend"}}}},
		DesiredZones: []environmentprojection.EnvironmentZoneProjection{{EnvironmentID: base.EnvironmentID,
			Desired: core.Zone{ID: ids.New(ids.KindNetwork), Name: "frontend", Subnet: "10.40.0.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: base.EnvironmentID}}},
		Volumes: []environmentprojection.EnvironmentVolumeIdentity{
			{ID: ids.New(ids.KindVolume), Slug: "app-data", Key: "app-data"},
		}}
	captureTestServiceProxySelection(t, &projection, serviceproxy.Caddy)
	base.Identities = mustComposeIdentitySnapshotFromProjection(t, projection)
	artifact, err := composerender.RenderCompose(base)
	if err != nil {
		t.Fatal(err)
	}
	projection.ComposeArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	prior := releaseTestWorkload("example/api:prior")
	result := releaserender.ReleaseRenderInput{ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		PlanID: base.PlanID, ArtifactID: ids.New(ids.KindConfig), PriorArtifactID: ids.New(ids.KindConfig),
		ServiceID: serviceID, ServiceName: "api", CandidateWorkload: releaseTestWorkload("example/api:next"), PriorWorkload: &prior,
		Strategy: strategy, PriorStrategy: strategy, CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		ProxyPorts: []domain.ProxyPort{
			{Port: 8080, Protocol: serviceports.TCP},
		}, ProxyGeneration: 2, PriorProxyGeneration: 1,
		ProxyImage: testServiceProxyImage(), ProxyImplementation: serviceproxy.Caddy,
		TenantID: base.TenantID, TenantSlug: "acme", ProjectID: base.ProjectID, ProjectSlug: "shop",
		EnvironmentID: base.EnvironmentID, EnvironmentName: "qa", AuthorizedVolumeDir: base.AuthorizedVolumeDir, Projection: projection}
	if strategy == domain.StrategyBlueGreen {
		result.Slot, result.PriorSlot = domain.SlotGreen, domain.SlotBlue
		result.CandidateTarget, result.PriorTarget = domain.WorkloadGreen, domain.WorkloadBlue
	}
	return result
}

// nativeRuntimeFixture captures a historical deployment's rendered bytes before
// a test changes desired state. Recovery consumes that capture, not this renderer.
func nativeRuntimeFixture(
	t *testing.T,
	source releaserender.ReleaseRenderInput,
	includeProxy bool,
) *agentpb.ComposeArtifact {
	t.Helper()
	project, err := composerender.LoadNormalizedEnvironmentProject(t.Context(), source.Projection)
	if err != nil {
		t.Fatal(err)
	}
	service := project.Services[source.ServiceName]
	if err := composerender.ApplySealedWorkload(&service, source.CandidateWorkload); err != nil {
		t.Fatal(err)
	}
	project.Services[source.ServiceName] = service
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: project, ArtifactID: source.ArtifactID,
		ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID:         source.TenantID, ProjectID: source.ProjectID, EnvironmentID: source.EnvironmentID,
		PlanID: source.PlanID, RenderGeneration: source.Projection.RenderGeneration,
		AuthorizedVolumeDir: source.AuthorizedVolumeDir,
		Identities:          mustComposeIdentitySnapshotFromProjection(t, source.Projection),
		Releases: map[string]composerender.ComposeReleaseIdentity{source.ServiceID: {
			ProxyImplementation: source.ProxyImplementation, ProxyImage: source.ProxyImage,
			ProxyAddresses: source.ProxyAddresses,
			ReleaseID:      source.ReleaseID, Target: source.CandidateTarget,
			Image: source.CandidateWorkload.LocalImageID, Strategy: source.Strategy,
			ServingReleaseID: source.ReleaseID, ServingTarget: source.CandidateTarget,
			ServingProxyGeneration: source.ProxyGeneration,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return nativeServiceArtifactFixture(t, artifact, source.ServiceID, string(source.CandidateTarget), includeProxy)
}

func nativeServiceArtifactFixture(
	t *testing.T,
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	target string,
	includeProxy bool,
) *agentpb.ComposeArtifact {
	t.Helper()
	selected := make(map[string]bool)
	services := artifact.Services[:0]
	for _, member := range artifact.Services {
		proxy := member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
		if member.ServiceId != serviceID || member.ExpectedReplicas == 0 ||
			proxy && !includeProxy || !proxy && member.Slot != "" && member.Slot != target {
			continue
		}
		selected[member.ComposeName] = true
		services = append(services, member)
	}
	artifact.Services = services
	var document yaml.Node
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	root := document.Content[0]
	mapping, err := composerender.ServiceArtifactMapping(root)
	if err != nil {
		t.Fatal(err)
	}
	content := mapping.Content[:0]
	for index := 0; index < len(mapping.Content); index += 2 {
		if selected[mapping.Content[index].Value] {
			content = append(content, mapping.Content[index], mapping.Content[index+1])
		}
	}
	mapping.Content = content
	artifact.CanonicalYaml, err = yaml.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	return artifact
}
