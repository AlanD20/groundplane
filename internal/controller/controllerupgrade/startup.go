package controllerupgrade

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RecoveryClaims interface {
	ListControllerTaskClaims(context.Context) ([]etcd.TaskAssignment, error)
}

// RestoreJournal rejects orphaned native handoff state before any listener
// opens. A journal is not independent execution authority: the durable Task
// claim must still exist and match its frozen operation.
func (coordinator *Coordinator) RestoreJournal(ctx context.Context, claims RecoveryClaims) error {
	if ctx == nil || claims == nil {
		return errs.New(errs.KindInternal, "native startup recovery claims are required")
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
	active, err := claims.ListControllerTaskClaims(ctx)
	if err != nil {
		return err
	}
	if len(active) != 1 || active[0].Task.Record.ID != journal.TaskID {
		return errs.New(errs.KindStateConflict, "native recovery journal has no matching durable Task claim")
	}
	return coordinator.Restore(ctx, active[0])
}
