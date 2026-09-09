package composehelper

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a slug-only edit may inspect the exact owned Docker Volume, but
// an absent or uninspectable object must never authorize creation or repair.
func TestManagedVolumeVerifyCannotCreateOrRepair(t *testing.T) {
	for _, state := range []string{"owned", "absent", "foreign"} {
		t.Run(state, func(t *testing.T) {
			request := volumeEnsureRequest(t)
			fake := &volumePreparationRunner{}
			if state != "absent" {
				if _, err := Execute(t.Context(), fake, request); err != nil {
					t.Fatal(err)
				}
				if state == "foreign" {
					fake.volume.Driver = "foreign"
				}
			}
			before := fake.creates
			request.Plan.Steps[0].GetManagedVolumeEnsure().RequireExisting = true
			request.Plan.PlanHash = nil
			var err error
			request.Plan, err = executionplan.Seal(request.Plan)
			if err != nil {
				t.Fatal(err)
			}
			response, err := Execute(t.Context(), fake, request)
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
