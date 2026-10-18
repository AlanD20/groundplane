package blueprintrelease

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	compose "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// BP-04/TASK-09: resolving runtime must not substitute a different Service,
// Component owner, digest or already-released workload under a candidate seal.
func TestNativeBlueprintCandidateRejectsChangedRuntimeAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*agentpb.ComposeArtifact)
	}{
		{name: "exact"},
		{name: "digest", change: func(a *agentpb.ComposeArtifact) { a.YamlSha256[0] ^= 1 }},
		{name: "environment", change: func(a *agentpb.ComposeArtifact) { a.OwnerId = ids.New(ids.KindEnvironment) }},
		{name: "service", change: func(a *agentpb.ComposeArtifact) { a.Services[0].ServiceId = ids.New(ids.KindService) }},
		{name: "component", change: func(a *agentpb.ComposeArtifact) { a.Services[0].OwnerComponentId = ids.New(ids.KindComponent) }},
		{name: "released", change: func(a *agentpb.ComposeArtifact) {
			a.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
		}},
		{name: "duplicate", change: func(a *agentpb.ComposeArtifact) { a.Services = append(a.Services, proto.CloneOf(a.Services[0])) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			input, artifact, seal := nativeCandidateRenderFixture(t)
			if test.change != nil {
				test.change(artifact)
			}
			var err error
			input.Projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
			if err != nil {
				t.Fatal(err)
			}
			service := input.Projection.DesiredServices[0].Desired.ID
			candidate, err := renderBlueprintCandidateArtifact(
				t.Context(),
				input,
				map[string]domain.WorkloadSeal{"api": seal},
				map[string]composerender.ComposeReleaseIdentity{service: {Strategy: domain.StrategyRecreate,
					ReleaseID: ids.New(ids.KindDeployment), Target: domain.WorkloadSingleton,
					ServingTarget: domain.WorkloadSingleton, Image: seal.LocalImageID}},
			)
			if test.change != nil {
				if err == nil || candidate != nil {
					t.Fatal("changed runtime authority rendered an executable candidate")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(candidate.Services) != 1 || candidate.Services[0].ImageReference != seal.LocalImageID ||
				candidate.Services[0].ServiceId != service || candidate.Services[0].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
				t.Fatal("candidate did not preserve its Service identity and sealed workload image")
			}
		})
	}
}

func nativeCandidateRenderFixture(
	t *testing.T,
) (releaserender.ReleaseRenderInput, *agentpb.ComposeArtifact, domain.WorkloadSeal) {
	t.Helper()
	tenant, project, environment := ids.New(ids.KindTenant), ids.New(ids.KindProject), ids.New(ids.KindEnvironment)
	service, zone := ids.New(ids.KindService), ids.New(ids.KindNetwork)
	composeProject := &compose.Project{Services: compose.Services{"api": {Name: "api", Image: "app:authored",
		Networks: map[string]*compose.ServiceNetworkConfig{"frontend": {}}}},
		Networks: compose.Networks{
			"frontend": {
				Name: "frontend",
				Ipam: compose.IPAMConfig{Config: []*compose.IPAMPool{{Subnet: "10.70.0.0/24"}}},
			},
		}}
	volumeDir := "/var/lib/groundplane/vol/" + tenant + "/" + project + "/" + environment
	input := releaserender.ReleaseRenderInput{TenantID: tenant, ProjectID: project, EnvironmentID: environment,
		PlanID: ids.New(ids.KindPlan), ArtifactID: ids.New(ids.KindConfig), AuthorizedVolumeDir: volumeDir,
		Projection: environmentprojection.EnvironmentComposeProjection{EnvironmentID: environment,
			RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
			DesiredServices: []services.EnvironmentServiceProjection{{EnvironmentID: environment,
				Desired: core.Service{ID: service, Name: "api", Image: "app:authored", Zones: []string{"frontend"}}}},
			DesiredZones: []environmentprojection.EnvironmentZoneProjection{{EnvironmentID: environment,
				Desired: core.Zone{
					ID:        zone,
					Name:      "frontend",
					Subnet:    "10.70.0.0/24",
					OwnerKind: core.ZoneOwnerEnvironment,
					OwnerID:   environment,
				}}}}}
	var err error
	input.Projection.NormalizedCompose, err = composerender.MarshalNormalizedEnvironmentProject(composeProject)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{Project: composeProject,
		ArtifactID: ids.New(ids.KindConfig), ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID: tenant, ProjectID: project, EnvironmentID: environment, PlanID: input.PlanID,
		RenderGeneration: 1, AuthorizedVolumeDir: volumeDir, Identities: composeidentity.Snapshot{
			Services: []composeidentity.Resource{
				{ID: service, Name: "api"},
			}, Networks: []composeidentity.Resource{{ID: zone, Name: "frontend"}}}})
	if err != nil {
		t.Fatal(err)
	}
	return input, artifact, domain.WorkloadSeal{
		RequestedReference: "app:candidate",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       1,
	}
}
