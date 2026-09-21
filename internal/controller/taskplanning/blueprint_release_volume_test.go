package taskplanning

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: retries cannot change the published resource selection or generate
// fresh preparation step identities from a different wall-clock timestamp.
func TestBlueprintVolumePreparationPreservesPublishedAuthority(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(reader.projection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	updated, steps, err := prepareBlueprintReleaseVolumes(task, artifact, "network-ready")
	if err != nil || len(steps) != len(artifact.Volumes) || len(steps) != 1 {
		t.Fatalf("prepare: steps=%v error=%v", steps, err)
	}
	if steps[0].GetManagedVolumeEnsure().GetVolumeId() != artifact.Volumes[0].VolumeId ||
		steps[0].PrerequisiteStepId != "network-ready" {
		t.Fatal("preparation lost its selected Volume or preceding resource barrier")
	}
	if _, mutated := task.Params[blueprintVolumePrepareStepsParam]; mutated {
		t.Fatal("preparation mutated the caller Task")
	}
	updated.CreatedAt = updated.CreatedAt.AddDate(0, 0, 1)
	_, replayed, err := prepareBlueprintReleaseVolumes(updated, artifact, "network-ready")
	if err != nil || !proto.Equal(steps[0], replayed[0]) {
		t.Fatalf("replay changed preparation: %v", err)
	}
	for _, encoded := range []string{"", "invalid", steps[0].StepId + "," + steps[0].StepId, ids.NewAt(ids.KindStep, task.CreatedAt, 900)} {
		updated.Params[blueprintVolumePrepareStepsParam] = encoded
		if _, _, err := prepareBlueprintReleaseVolumes(updated, artifact, "network-ready"); err == nil {
			t.Fatalf("accepted changed published preparation %q", encoded)
		}
	}
}
