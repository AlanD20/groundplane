package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a resource-only operation cannot manufacture volume authority from
// a valid id alone, select another owner, or escape the managed directory leaf.
func TestManagedVolumeEnsureBindsOwnedManagedSelection(t *testing.T) {
	const id = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, name := range []string{"valid", "absent", "missing-volume", "owner", "directory", "leaf", "physical-name", "operation"} {
		t.Run(name, func(t *testing.T) {
			ensure := &agentpb.ManagedVolumeEnsure{ArtifactId: "artifact", VolumeId: id}
			volume := &agentpb.ComposeVolume{
				VolumeId:    id,
				ComposeName: "data",
				DockerName:  "gp_vol_vol_01arz3ndektsv4rrffq69g5fav",
			}
			artifact := &agentpb.ComposeArtifact{OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
				AuthorizedVolumeDir: "/var/lib/groundplane/vol/test", Volumes: []*agentpb.ComposeVolume{volume}}
			operation := agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
			switch name {
			case "absent":
				ensure.ArtifactId = "missing"
			case "missing-volume":
				artifact.Volumes = nil
			case "owner":
				artifact.OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_UNSPECIFIED
			case "directory":
				artifact.AuthorizedVolumeDir = ""
			case "leaf":
				volume.ComposeName = "../other"
			case "physical-name":
				volume.DockerName = "foreign"
			case "operation":
				operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
			}
			err := validateManagedVolumeEnsure(
				operation,
				ensure,
				map[string]*agentpb.ComposeArtifact{"artifact": artifact},
			)
			if (name == "valid") != (err == nil) {
				t.Fatalf("validation=%v", err)
			}
		})
	}
}
