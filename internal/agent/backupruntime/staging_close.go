package backupruntime

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Close follows cancellation and joining of all source workers. It releases
// descriptors but preserves staged bytes for the next Controller disposition;
// losing a connection is not authority to delete an incomplete Backup.
func (state *Staging) Close(ctx context.Context) error {
	if state == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.closed = true
	stages := make(map[*backupstage.Stage]struct{}, len(state.prepared)+len(state.active))
	for _, prepared := range state.prepared {
		stages[prepared.Stage] = struct{}{}
	}
	for _, held := range state.held {
		stages[held.Stage] = struct{}{}
	}
	for _, stage := range state.active {
		stages[stage] = struct{}{}
	}
	var result error
	for stage := range stages {
		result = errors.Join(result, stage.Suspend(ctx))
	}
	result = errors.Join(result, state.stager.Close(ctx))
	if result != nil {
		return errs.Wrap(errs.KindStorageUnavailable, result)
	}
	clear(state.prepared)
	clear(state.held)
	clear(state.active)
	return nil
}
