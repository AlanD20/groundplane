package taskplanning

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	compose "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Shared captured source for the release/hook fixtures still being migrated.
// This is test data, not a production compatibility renderer.
func freezeBlueprintNativeRuntimeFixture(
	t *testing.T,
	reader *blueprintPlanReader,
	task etcd.TaskRecord,
	project *compose.Project,
) {
	t.Helper()
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: project, ArtifactID: task.Params[taskcontract.EnvironmentBlueprintArtifactParam],
		ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID:         reader.tenant.ID, ProjectID: reader.project.ID, EnvironmentID: reader.environment.ID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration), AuthorizedVolumeDir: reader.environment.VolumeDir,
		Identities: mustComposeIdentitySnapshotFromProjection(t, reader.projection)})
	if err != nil {
		t.Fatal(err)
	}
	reader.projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
}
