package controllerupgrade

import (
	"context"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
	if err := coordinator.RestoreJournal(context.Background(), startupClaims{h.claim}); err != nil {
		t.Fatal(err)
	}
	if coordinator.active == nil || coordinator.active.expected.TaskID != h.claim.Task.Record.ID {
		t.Fatal("matching journal did not restore admission")
	}
	h.store.journal.Phase = upgrade.PhaseCancelled
	if err := coordinator.RestoreJournal(context.Background(), startupClaims{}); err != nil {
		t.Fatalf("settled history blocked startup: %v", err)
	}
}

type startupClaims []etcd.TaskAssignment

func (claims startupClaims) ListControllerTaskClaims(context.Context) ([]etcd.TaskAssignment, error) {
	return claims, nil
}
