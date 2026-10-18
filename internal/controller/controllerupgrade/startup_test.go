package controllerupgrade

import (
	"context"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a host handoff journal alone must not bypass durable Task authority
// or reopen Agent dispatch when the owning claim is missing after interruption.
func TestNativeStartupJournalRequiresMatchingClaim(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.store.prepareExpected(h.expected)
	coordinator := h.coordinator(t, h.input.PreviousController)
	if err := coordinator.RestoreJournal(context.Background(), startupClaims{}); err == nil {
		t.Fatal("orphaned journal accepted")
	}
	if err := coordinator.RestoreJournal(context.Background(), startupClaims{h.expected}); err != nil {
		t.Fatal(err)
	}
	if coordinator.active == nil || coordinator.active.expected.TaskID != h.expected.TaskID {
		t.Fatal("matching journal did not restore admission")
	}
	h.store.journal.Phase = upgrade.PhaseCancelled
	if err := coordinator.RestoreJournal(context.Background(), startupClaims{}); err != nil {
		t.Fatalf("settled history blocked startup: %v", err)
	}
}

type startupClaims []upgrade.Journal

func (claims startupClaims) ControllerRecovery(_ context.Context, taskID string) (upgrade.Journal, error) {
	for _, journal := range claims {
		if journal.TaskID == taskID {
			return journal, nil
		}
	}
	return upgrade.Journal{}, errs.New(errs.KindStateConflict, "captured native work is missing")
}
