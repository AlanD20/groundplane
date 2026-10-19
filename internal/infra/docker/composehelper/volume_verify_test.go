package composehelper

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a slug-only edit may inspect the exact owned Docker Volume, but
// an absent or uninspectable object must never authorize creation or repair.
func TestManagedVolumeVerifyCannotCreateOrRepair(t *testing.T) {
	for _, state := range []string{"owned", "absent", "foreign"} {
		t.Run(state, func(t *testing.T) {
			artifact := volumeEnsureArtifact(t)
			fake := &volumePreparationRunner{}
			if state != "absent" {
				if _, err := prepareCapturedVolume(t.Context(), fake, artifact, artifact.Volumes[0].VolumeId, false); err != nil {
					t.Fatal(err)
				}
				if state == "foreign" {
					fake.volume.Driver = "foreign"
				}
			}
			before := fake.creates
			response, err := prepareCapturedVolume(t.Context(), fake, artifact, artifact.Volumes[0].VolumeId, true)
			success := err == nil &&
				response.GetOutcome() == agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED
			if success != (state == "owned") || fake.creates != before {
				t.Fatalf(
					"read-only verification state=%s success=%t creates=%d before=%d error=%v",
					state,
					success,
					fake.creates,
					before,
					err,
				)
			}
		})
	}
}
