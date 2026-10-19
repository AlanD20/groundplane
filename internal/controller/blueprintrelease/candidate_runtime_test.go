package blueprintrelease

import (
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/proto/agentpb"
	compose "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// BP-04 / TASK-75: the same stable Service owns a proxy and a workload, but only
// the workload may receive the candidate application image and Release labels.
func TestNativeBlueprintAddressableCandidateSeparatesProxyAndWorkload(t *testing.T) {
	input, _, seal := nativeCandidateRenderFixture(t)
	project, err := composerender.LoadNormalizedEnvironmentProject(t.Context(), input.Projection)
	if err != nil {
		t.Fatal(err)
	}
	api := project.Services["api"]
	api.Expose = []string{"8080/tcp"}
	project.Services["api"] = api
	input.Projection.NormalizedCompose, err = composerender.MarshalNormalizedEnvironmentProject(project)
	if err != nil {
		t.Fatal(err)
	}
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(input.Projection)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := composerender.RenderCompose(composerender.ComposeRenderInput{Project: project,
		ArtifactID: ids.New(ids.KindConfig), ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID: input.TenantID, ProjectID: input.ProjectID, EnvironmentID: input.EnvironmentID,
		PlanID: input.PlanID, RenderGeneration: 1, AuthorizedVolumeDir: input.AuthorizedVolumeDir, Identities: identities})
	if err != nil {
		t.Fatal(err)
	}
	input.Projection.ComposeArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(resolved)
	if err != nil {
		t.Fatal(err)
	}
	image := routerServiceTestImage()
	platform, reference, ok := image.Select("linux", "amd64")
	if !ok {
		t.Fatal("fixture has no amd64 proxy")
	}
	serviceID, releaseID := input.Projection.DesiredServices[0].Desired.ID, ids.New(ids.KindDeployment)
	candidate, err := renderBlueprintCandidateArtifact(t.Context(), input,
		map[string]domain.WorkloadSeal{"api": seal}, map[string]composerender.ComposeReleaseIdentity{serviceID: {
			Strategy: domain.StrategyRecreate, ReleaseID: releaseID, Target: domain.WorkloadSingleton,
			ServingTarget: domain.WorkloadSingleton, ServingReleaseID: releaseID, ServingProxyGeneration: 1,
			Image: seal.LocalImageID, ProxyImplementation: serviceproxy.Caddy,
			ProxyImage: &domain.ProxyImage{
				Repository:  image.Repository,
				IndexDigest: image.IndexDigest,
				Platform:    platform,
			},
		}})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.Services) != 2 {
		t.Fatal("addressable candidate did not render one proxy and one workload")
	}
	proxyCount, workloadCount := 0, 0
	for _, member := range candidate.Services {
		if member.ServiceId != serviceID {
			t.Fatal("candidate replaced stable Service identity")
		}
		switch member.Role {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
			proxyCount++
			if member.ImageReference != reference || len(member.ProxyConfigJson) == 0 {
				t.Fatal("proxy received the application image or lost serving configuration")
			}
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
			workloadCount++
			if member.ImageReference != seal.LocalImageID {
				t.Fatal("workload lost the sealed candidate image")
			}
		default:
			t.Fatal("candidate acquired an unexpected runtime role")
		}
	}
	if proxyCount != 1 || workloadCount != 1 {
		t.Fatal("stable Service identity confused proxy and workload selection")
	}
}

// BP-04 / TASK-75: candidate rendering must retain resolved Entries, env files,
// Attach networks and generated Component runtime instead of returning to the
// authored Blueprint, which has neither their values nor the generated Service.
func TestNativeBlueprintCandidatePreservesResolvedRuntime(t *testing.T) {
	input, _, seal := nativeCandidateRenderFixture(t)
	project, err := composerender.LoadNormalizedEnvironmentProject(t.Context(), input.Projection)
	if err != nil {
		t.Fatal(err)
	}
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(input.Projection)
	if err != nil {
		t.Fatal(err)
	}
	value := "captured-runtime-$GP_LITERAL"
	api := project.Services["api"]
	api.Environment = compose.MappingWithEquals{"GP_RESOLVED_ENTRY": &value}
	api.EnvFiles = []compose.EnvFile{{Path: "/var/lib/groundplane/runtime/captured.env", Required: true}}
	attachID := ids.New(ids.KindNetwork)
	attachName := "gp_attach_" + strings.ToLower(attachID)
	api.Networks[attachName] = &compose.ServiceNetworkConfig{}
	project.Services["api"] = api
	project.Networks[attachName] = compose.NetworkConfig{External: true, Name: "gp_net_" + attachID}
	image := routerServiceTestImage()
	platform, reference, supported := image.Select("linux", "amd64")
	if !supported {
		t.Fatal("fixture has no amd64 image")
	}
	selected := composeidentity.ComponentImage{Repository: image.Repository, IndexDigest: image.IndexDigest,
		Reference: reference, Platform: platform}
	componentID, generatedID := ids.New(ids.KindComponent), ids.New(ids.KindService)
	project.Services["router"] = compose.ServiceConfig{Name: "router", Image: reference,
		Networks: map[string]*compose.ServiceNetworkConfig{"frontend": {}},
		Command:  compose.ShellCommand{"caddy", "run"}}
	identities.Services = append(identities.Services, composeidentity.Resource{ID: generatedID, Name: "router",
		ComponentID: componentID, ComponentImage: &selected})
	resolved, err := composerender.RenderCompose(composerender.ComposeRenderInput{Project: project,
		ArtifactID: ids.New(ids.KindConfig), ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID: input.TenantID, ProjectID: input.ProjectID, EnvironmentID: input.EnvironmentID,
		PlanID: input.PlanID, RenderGeneration: 1, AuthorizedVolumeDir: input.AuthorizedVolumeDir,
		Identities: identities, ExternalNetworks: []composeidentity.Resource{{ID: attachID, Name: attachName}}})
	if err != nil {
		t.Fatal(err)
	}
	input.Projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(resolved)
	if err != nil {
		t.Fatal(err)
	}
	input.Projection.Components = []components.Record{{Desired: components.DesiredRecord{ID: componentID},
		Runtime: components.RuntimeRecord{GeneratedServices: []string{generatedID}}}}
	serviceID := input.Projection.DesiredServices[0].Desired.ID
	candidate, err := renderBlueprintCandidateArtifact(t.Context(), input,
		map[string]domain.WorkloadSeal{"api": seal},
		map[string]composerender.ComposeReleaseIdentity{serviceID: {Strategy: domain.StrategyRecreate,
			ReleaseID: ids.New(ids.KindDeployment), Target: domain.WorkloadSingleton,
			ServingTarget: domain.WorkloadSingleton, Image: seal.LocalImageID}})
	if err != nil {
		t.Fatal(err)
	}
	projection := input.Projection
	projection.NormalizedCompose = candidate.CanonicalYaml
	runtime, err := composerender.LoadNormalizedEnvironmentProject(t.Context(), projection)
	if err != nil {
		t.Fatal(err)
	}
	consumer := runtime.Services["api"]
	if consumer.Image != seal.LocalImageID || consumer.Environment["GP_RESOLVED_ENTRY"] == nil ||
		*consumer.Environment["GP_RESOLVED_ENTRY"] != value ||
		!slices.Equal(consumer.EnvFiles, api.EnvFiles) || consumer.Networks[attachName] == nil ||
		!bool(runtime.Networks[attachName].External) || runtime.Networks[attachName].Name != "gp_net_"+attachID {
		t.Fatalf("candidate lost resolved consumer input: %+v", consumer)
	}
	if runtime.Services["router"].Image != reference ||
		!slices.Equal(runtime.Services["router"].Command, compose.ShellCommand{"caddy", "run"}) {
		t.Fatal("native candidate image replaced generated Component runtime")
	}
	if len(candidate.Services) != 2 {
		t.Fatal("candidate lost or duplicated a runtime member")
	}
	for _, metadata := range candidate.Services {
		if metadata.ServiceId != generatedID {
			continue
		}
		original := resolved.Services[slices.IndexFunc(resolved.Services, func(service *agentpb.ComposeService) bool {
			return service.ServiceId == generatedID
		})]
		if !proto.Equal(metadata, original) {
			t.Fatal("generated Component acquired native Release authority")
		}
	}
}
