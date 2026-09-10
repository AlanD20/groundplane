package controllerupgrade

import (
	"context"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: every unfinished phase, including predecessor recovery, retains
// ordinary mutation isolation; only the exact native Task's Abort may proceed.
func TestNativeMutationAdmissionFollowsValidatedJournal(t *testing.T) {
	for _, phase := range []upgrade.Phase{
		upgrade.PhasePrepared, upgrade.PhaseActivating, upgrade.PhaseTrial, upgrade.PhaseStarting,
		upgrade.PhaseRollingBack, upgrade.PhaseStopping, upgrade.PhaseRolledBack,
		upgrade.PhaseHealthy, upgrade.PhaseRecovered, upgrade.PhaseCancelled,
	} {
		t.Run(string(phase), func(t *testing.T) {
			h := newServiceHarness(t)
			h.catalog.journal, h.catalog.found = h.expected, true
			h.catalog.journal.Phase = phase
			h.catalog.journal.TrialBootID = "12345678-1234-1234-1234-123456789abc"
			for _, taskID := range []string{"", "task_01ARZ3NDEKTSV4RRFFQ69G5FAW", h.expected.TaskID} {
				err := h.service.CheckMutation(context.Background(), taskID)
				allowed := phase.Settled() || taskID == h.expected.TaskID
				if (err == nil) != allowed {
					t.Fatalf("phase=%s target=%s, error=%v", phase, taskID, err)
				}
			}
			h.catalog.journal.Schema++
			if h.service.CheckMutation(context.Background(), h.expected.TaskID) == nil {
				t.Fatal("invalid journal opened admission")
			}
		})
	}
	h := newServiceHarness(t)
	h.catalog.failure = errs.New(errs.KindInternal, "journal unavailable")
	if h.service.CheckMutation(context.Background(), "") == nil {
		t.Fatal("unavailable journal opened admission")
	}
	h.catalog.failure = nil
	if err := h.service.CheckMutation(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	h.service.catalog = nil
	if err := h.service.CheckMutation(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
}
