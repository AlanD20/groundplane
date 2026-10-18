package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// RetireCompletedAttach collects older failed/inspection journals only after
// the Controller acknowledged successful removal of the same captured work.
// Keep the successful receipt until all predecessors are gone so a crash can
// resume this bounded sweep without needing deleted recovery evidence.
func (journal *Journal) RetireCompletedAttach(ctx context.Context, terminal workexecution.Attempt) error {
	if err := journal.retireAttachPredecessors(ctx, terminal); err != nil {
		return err
	}
	return journal.Retire(ctx, terminal)
}

func (journal *Journal) retireAttachPredecessors(ctx context.Context, terminal workexecution.Attempt) error {
	if terminal.Validate() != nil || terminal.Direction != workexecution.Forward || terminal.FinishedAt == nil ||
		terminal.Result == nil || terminal.Outcome != workexecution.Succeeded {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := journal.readRecord(ctx, terminal.ID)
	if err != nil {
		return err
	}
	if current == nil {
		return nil // A previous complete sweep removed this receipt last.
	}
	if current.AttachRemoval == nil || current.AttachRemoval.Completion == nil || current.AttachFailure != nil ||
		!workexecution.SameAssignment(current.Attempt, terminal) {
		return conflict()
	}
	saved := current.AttachRemoval.Completion
	if saved.Result == nil || *saved.Result != *terminal.Result || saved.FinishedAt == nil || !saved.FinishedAt.Equal(*terminal.FinishedAt) {
		return conflict()
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return err
	}
	for attemptID, committed := range attempts {
		if !committed || attemptID == terminal.ID {
			continue
		}
		previous, err := journal.readRecord(ctx, attemptID)
		if err != nil {
			return err
		}
		if previous == nil || previous.Attempt.TaskID != terminal.TaskID || previous.Attempt.WorkItemID != terminal.WorkItemID {
			continue
		}
		if previous.Attempt.Input != terminal.Input || previous.Attempt.Executor.HostID != terminal.Executor.HostID ||
			previous.Attempt.Number >= terminal.Number {
			return conflict()
		}
		switch {
		case previous.AttachRecovery != nil:
			if previous.AttachRecovery.Source.Failure.Authority != current.AttachRemoval.Authority {
				return conflict()
			}
		case previous.AttachRemoval != nil:
			if previous.AttachRemoval.Authority != current.AttachRemoval.Authority || previous.AttachRemoval.Completion != nil {
				return conflict()
			}
		case previous.AttachFailure != nil:
			// An unstarted failure has no command identities; final success of
			// the exact same work supersedes its local delivery requirement.
		default:
			return conflict()
		}
		if err := journal.removeRecord(ctx, attemptID); err != nil {
			return err
		}
	}
	return nil
}
