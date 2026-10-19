package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// Sequence includes hook checkpoints, not only the last Release cursor. It
// detects lost local execution state before a reconnect can start more work.
func (journal *Journal) Sequence(ctx context.Context, attempt workexecution.Attempt) (uint64, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return 0, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return 0, err
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) || state.Attempt.StartedAt == nil ||
		attempt.StartedAt == nil ||
		!state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return 0, conflict()
	}
	return state.Sequence, nil
}
