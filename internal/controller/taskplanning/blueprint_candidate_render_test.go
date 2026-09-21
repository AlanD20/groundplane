package taskplanning

import (
	"testing"

	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskcontract "github.com/AlanD20/groundplane/internal/controller/taskcontract"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: using resolved fields must never permit a frozen source to
// substitute another Service, Component owner, or already-released workload.
func TestBlueprintCandidateRenderRejectsChangedRuntimeAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*agentpb.ComposeArtifact)
	}{
		{name: "exact"},
		{name: "digest", change: func(a *agentpb.ComposeArtifact) { a.YamlSha256[0] ^= 1 }},
		{name: "environment", change: func(a *agentpb.ComposeArtifact) { a.OwnerId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW" }},
		{name: "service", change: func(a *agentpb.ComposeArtifact) { a.Services[0].ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW" }},
		{name: "component", change: func(a *agentpb.ComposeArtifact) { a.Services[0].OwnerComponentId = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW" }},
		{name: "released", change: func(a *agentpb.ComposeArtifact) {
			a.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
		}},
		{name: "duplicate", change: func(a *agentpb.ComposeArtifact) { a.Services = append(a.Services, proto.CloneOf(a.Services[0])) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader, task := blueprintPlanTestState(t)
			artifact := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(reader.projection.ComposeArtifact, artifact); err != nil {
				t.Fatal(err)
			}
			if test.change != nil {
				test.change(artifact)
			}
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
			if err != nil {
				t.Fatal(err)
			}
			reader.projection.ComposeArtifact = encoded
			seal := releaseTestWorkload("example/api:next")
			input := testreleaserender.ReleaseRenderInput{
				PlanID: task.PlanID, ArtifactID: task.Params[testtaskcontract.EnvironmentBlueprintArtifactParam],
				TenantID: reader.tenant.ID, ProjectID: reader.project.ID, EnvironmentID: reader.environment.ID,
				AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection,
			}
			_, err = renderBlueprintCandidateArtifact(t.Context(), task, input,
				map[string]domain.WorkloadSeal{"api": seal}, map[string]testcomposerender.ComposeReleaseIdentity{
					"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV": {Strategy: domain.StrategyRecreate,
						ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW", Target: domain.WorkloadSingleton,
						ServingTarget: domain.WorkloadSingleton, Image: seal.LocalImageID},
				})
			if (err != nil) != (test.change != nil) {
				t.Fatalf("render error = %v", err)
			}
		})
	}
}

// Fixture changes must prepare the same resolved runtime that the real
// Blueprint producer passes to candidate rendering, not just authored YAML.
func freezeBlueprintNativeRuntimeFixture(
	t *testing.T,
	reader *blueprintPlanReader,
	task etcd.TaskRecord,
	project *composetypes.Project,
) {
	t.Helper()
	artifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
		Project: project, ArtifactID: task.Params[testtaskcontract.EnvironmentBlueprintArtifactParam],
		ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant,
		TenantID:         reader.tenant.ID, ProjectID: reader.project.ID, EnvironmentID: reader.environment.ID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration), AuthorizedVolumeDir: reader.environment.VolumeDir,
		Identities: mustComposeIdentitySnapshotFromProjection(t, reader.projection),
	})
	if err != nil {
		t.Fatal(err)
	}
	reader.projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
}
