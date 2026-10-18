package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// StartBackup persists the accepted start before requesting credentials or
// running a source. Its empty source list means no local result was recorded,
// not proof that an external effect is absent after interruption.
func (journal *Journal) StartBackup(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	attempt workexecution.Attempt, at time.Time,
) error {
	owned, decoded, err := executionplan.OpenBackupWorkAssignment(assignment, attempt.Executor)
	if err != nil {
		return err
	}
	if !workexecution.SameAssignment(decoded, attempt) || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		at.Before(*attempt.StartedAt) {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state != nil {
		return validateBackupState(state, attempt, owned.Input)
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return err
	}
	if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
		return conflict()
	}
	return journal.writeRecord(ctx, attemptRecord{Attempt: attempt, Sequence: 1, UpdatedAt: at.UTC(),
		Backup: &BackupRecord{Sources: make([]BackupSourceReport, len(owned.Input.Steps))}})
}
