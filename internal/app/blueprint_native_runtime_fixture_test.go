package app

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// renderNativeRuntimeFixture captures already rendered historical Service
// bytes for a test. Production recovery consumes the stored capture and never
// invokes this renderer.
func renderNativeRuntimeFixture(
	t *testing.T,
	source testreleaserender.ServiceLifecycleRelease,
) []*agentpb.ComposeArtifact {
	t.Helper()
	result := []*agentpb.ComposeArtifact{
		renderNativeServiceRuntimeFixture(t, source.Current, true),
	}
	if source.RetainedPrior != nil {
		result = append(result, renderNativeServiceRuntimeFixture(t, *source.RetainedPrior, false))
	}
	return result
}

func renderNativeServiceRuntimeFixture(
	t *testing.T,
	source testreleaserender.ReleaseRenderInput,
	includeProxy bool,
) *agentpb.ComposeArtifact {
	t.Helper()
	project, err := testcomposerender.LoadNormalizedEnvironmentProject(t.Context(), source.Projection)
	if err != nil {
		t.Fatal(err)
	}
	service := project.Services[source.ServiceName]
	if err := testcomposerender.ApplySealedWorkload(&service, source.CandidateWorkload); err != nil {
		t.Fatal(err)
	}
	project.Services[source.ServiceName] = service
	identities := make([]testcomposeidentity.Resource, len(source.Projection.DesiredServices))
	for index, desired := range source.Projection.DesiredServices {
		identities[index] = testcomposeidentity.Resource{ID: desired.Desired.ID, Name: desired.Desired.Name}
	}
	artifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
		Project: project, ArtifactID: source.ArtifactID,
		ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant,
		TenantID:         source.TenantID, ProjectID: source.ProjectID, EnvironmentID: source.EnvironmentID,
		PlanID: source.PlanID, RenderGeneration: source.Projection.RenderGeneration,
		AuthorizedVolumeDir: source.AuthorizedVolumeDir,
		Identities:          testcomposeidentity.Snapshot{Services: identities},
		Releases: map[string]testcomposerender.ComposeReleaseIdentity{source.ServiceID: {
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
	selected := make(map[string]bool)
	services := artifact.Services[:0]
	for _, member := range artifact.Services {
		proxy := member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
		if member.ServiceId != source.ServiceID || member.ExpectedReplicas == 0 ||
			(proxy && !includeProxy) || (!proxy && member.Slot != "" && member.Slot != string(source.CandidateTarget)) {
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
	mapping, err := testcomposerender.ServiceArtifactMapping(root)
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

// captureNativeRuntimeFixture turns earlier rendered physical bytes into the
// acknowledged receipt consumed by retention. It models a completed fixture
// deployment; production only reads the already persisted receipt.
func captureNativeRuntimeFixture(
	t *testing.T,
	current *agentpb.ComposeArtifact,
	retainedPrior *agentpb.ComposeArtifact,
) serviceruntimerecord.Record {
	t.Helper()
	if current == nil {
		t.Fatal("native runtime fixture is absent")
	}
	runtime := executionplan.CandidateRuntime{}
	var renderGeneration uint64
	for _, member := range current.Services {
		if member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			runtime.ProxyConfigSHA256 = append([]byte(nil), member.ProxyConfigSha256...)
			continue
		}
		runtime.ServiceID = member.ServiceId
		switch member.Role {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
			runtime.Target = member.Slot
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA:
			runtime.Target = "rolling"
		default:
			runtime.Target = "singleton"
		}
		for _, label := range member.ExpectedLabels {
			if label.Key == "com.groundplane.release-id" {
				runtime.ReleaseID = label.Value
			}
			if label.Key == "com.groundplane.render-generation" {
				generation, err := strconv.ParseUint(label.Value, 10, 64)
				if err != nil || generation == 0 || strconv.FormatUint(generation, 10) != label.Value {
					t.Fatal("native runtime fixture has invalid render generation")
				}
				if renderGeneration != 0 && renderGeneration != generation {
					t.Fatal("native runtime fixture has inconsistent render generations")
				}
				renderGeneration = generation
			}
		}
	}
	if renderGeneration == 0 {
		t.Fatal("native runtime fixture has no render generation")
	}
	for _, member := range current.Services {
		if member.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		generation, err := executionplan.ProxyConfigGeneration(
			member.ProxyImplementation,
			member.ProxyConfigJson,
			runtime.ReleaseID,
		)
		if err != nil {
			t.Fatal(err)
		}
		runtime.ProxyGeneration = generation
	}
	var err error
	runtime.CurrentArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if retainedPrior != nil {
		runtime.RetainedPriorArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(retainedPrior)
		if err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256([]byte("completed native runtime fixture"))
	record := serviceruntimerecord.Record{
		EnvironmentID: current.OwnerId,
		Runtime:       runtime,
		Source: serviceruntimerecord.Acknowledgement{
			TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			StepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", AgentID: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			AssignmentID: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionEpoch: 1,
			RenderGeneration: renderGeneration,
			PlanHash:         hex.EncodeToString(digest[:]), EffectDigest: hex.EncodeToString(digest[:]),
			AcknowledgedAt: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC),
		},
	}
	if err := serviceruntimerecord.Validate(record); err != nil {
		t.Fatal(err)
	}
	return record
}
