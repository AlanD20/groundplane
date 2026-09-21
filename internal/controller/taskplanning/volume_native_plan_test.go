package taskplanning

import (
	"bytes"
	"testing"

	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: use the real retained native renderer, including its proxy seal and
// image/Release identities, to prove resource-only mutation plans admit the
// unchanged runtime rather than only simplified authored test metadata.
func TestVolumeResourcePlanPreservesRenderedNativeRelease(t *testing.T) {
	_, _, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	render := input.Members[0].Render
	baseline := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(render.PriorRuntime.CurrentArtifact, baseline); err != nil {
		t.Fatal(err)
	}
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
		steps := []*agentpb.ExecutionStep{}
		if action == testcomposerender.VolumeArtifactAdd {
			steps = append(steps, &agentpb.ExecutionStep{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FC6", TimeoutSeconds: 30,
				Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
					ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
						ArtifactId: artifactID, VolumeIds: []string{volumeID}, IntentSha256: bytes.Repeat([]byte{1}, 32),
					},
				}})
		}
		steps = append(steps, &agentpb.ExecutionStep{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FC7", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
				ArtifactId: artifactID, VolumeId: volumeID, RequireExisting: action == testcomposerender.VolumeArtifactEdit,
			}}})
		if _, err := testtaskplan.Build(testtaskplan.BuildInput{VolumeRoot: "/var/lib/groundplane/vol", PlanID: planID,
			RenderGeneration: generation, Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetID: volumeID,
			Artifacts: []*agentpb.ComposeArtifact{candidate}, Steps: steps}); err != nil {
			t.Fatalf("%s native resource-only plan: %v", action, err)
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
