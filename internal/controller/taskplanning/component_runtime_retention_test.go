package taskplanning

import (
	"testing"

	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the second render used for a native Blueprint candidate must not
// overwrite the source ownership retained before immutable desired staging.
func TestBlueprintCandidateRetainsFrozenComponentRuntime(t *testing.T) {
	reader, _, task := routeRemovalPlanTestState(t)
	projection := reader.projection
	task.RenderGeneration = int32(projection.RenderGeneration + 1)
	prior := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, prior); err != nil {
		t.Fatal(err)
	}
	input := testreleaserender.ReleaseRenderInput{
		PlanID: task.PlanID, ArtifactID: prior.ArtifactId,
		TenantID: reader.tenant.ID, ProjectID: reader.project.ID, EnvironmentID: reader.environment.ID,
		AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: projection,
	}
	result, err := renderBlueprintCandidateArtifact(t.Context(), task, input, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, before := range prior.Services {
		if before.OwnerComponentId == "" {
			continue
		}
		found := false
		for _, after := range result.Services {
			if after.ServiceId == before.ServiceId {
				found = proto.Equal(after, before)
			}
		}
		if !found {
			t.Fatal("candidate rehydration changed frozen Component runtime ownership")
		}
	}
}
