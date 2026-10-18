package controllerupgrade

import (
	"context"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoverySource resolves a local journal through its exact native work and
// first assignment, including protection retained for explicit cleanup Retry.
type RecoverySource interface {
	ControllerRecovery(context.Context, string) (upgrade.Journal, error)
}

// RestoreJournal establishes admission before any Agent listener opens. A local
// journal cannot invent execution authority or translate a retired Task format.
func (coordinator *Coordinator) RestoreJournal(ctx context.Context, source RecoverySource) error {
	if ctx == nil || source == nil {
		return errs.New(errs.KindInternal, "native startup recovery source is required")
	}
	journal, found, err := coordinator.journal.Current(ctx)
	if err != nil || !found {
		return err
	}
	if err := journal.Validate(); err != nil {
		return err
	}
	if journal.Phase.Settled() {
		return nil
	}
	expected, err := source.ControllerRecovery(ctx, journal.TaskID)
	if err != nil {
		return err
	}
	if !expected.SameOperation(journal) {
		return errs.New(errs.KindStateConflict, "native handoff journal has no matching captured work")
	}
	return coordinator.Restore(ctx, expected)
}
