package taskplanning

import (
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"gopkg.in/yaml.v3"
)

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
