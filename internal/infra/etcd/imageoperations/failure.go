package imageoperations

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// RejectForward closes only work with no unresolved destructive/tag effect.
// Fetching may have cached immutable content, which is retained as ordinary image
// inventory. It must be called only after the local fetch call has returned.
func (journal *Journal) RejectForward(ctx context.Context, outcome workexecution.Outcome, at time.Time) error {
	if outcome != workexecution.Failed && outcome != workexecution.Cancelled && outcome != workexecution.Superseded {
		return invalid()
	}
	owned, err := journal.read(ctx, false)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Ready && record.Phase != Fetching {
		return conflict()
	}
	record.Phase, record.AttemptID, record.Outcome = Finished, journal.attemptID, outcome
	record.UpdatedAt, record.FinishedAt = at, &at
	return journal.commit(ctx, owned, record, workqueue.Change{})
}

func (journal *Journal) RejectAssigned(ctx context.Context, outcome workexecution.Outcome, at time.Time) error {
	if outcome != workexecution.Failed && outcome != workexecution.Cancelled && outcome != workexecution.Superseded {
		return invalid()
	}
	owned, err := journal.readState(ctx, false, true)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Ready {
		return conflict()
	}
	record.Phase, record.AttemptID, record.Outcome = Finished, journal.attemptID, outcome
	record.UpdatedAt, record.FinishedAt = at, &at
	return journal.commit(ctx, owned, record, workqueue.Change{})
}
