package taskplanning

import (
	"bytes"
	"testing"

	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"google.golang.org/protobuf/proto"
)

// TASK-72: use the real retained native renderer, including its proxy seal and
// image/Release identities, to prove resource-only mutations preserve the
// unchanged runtime rather than only simplified authored test metadata.
func TestVolumeResourceMutationPreservesRenderedNativeRelease(t *testing.T) {
	render := nativeRetainedRenderFixture(t, domain.StrategyBlueGreen)
	baseline := nativeRuntimeFixture(t, render, true)
	if len(baseline.Services) != 2 {
		t.Fatal("fixture is not a retained proxy and serving slot")
	}
	const volumeID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FC3"
	const planID = "plan_01ARZ3NDEKTSV4RRFFQ69G5FC4"
	const artifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC5"
	generation := render.Projection.RenderGeneration + 1
	current := baseline
	for _, action := range []testcomposerender.VolumeArtifactAction{testcomposerender.VolumeArtifactAdd, testcomposerender.VolumeArtifactEdit} {
		candidate, err := testcomposerender.MutateEnvironmentVolumeArtifact(
			current,
			testcomposerender.VolumeArtifactMutation{Action: action,
				VolumeID: volumeID, Key: "extra-data", ArtifactID: artifactID, PlanID: planID,
				TenantID: render.TenantID, ProjectID: render.ProjectID, RenderGeneration: generation},
		)
		if err != nil {
			t.Fatal(err)
		}
		for index, service := range baseline.Services {
			if !proto.Equal(service, candidate.Services[index]) {
				t.Fatal("Volume identity changed the retained native Release")
			}
		}
		if action == testcomposerender.VolumeArtifactEdit &&
			!bytes.Equal(current.CanonicalYaml, candidate.CanonicalYaml) {
			t.Fatal("slug edit changed runtime YAML")
		}
		current = candidate
	}
}
