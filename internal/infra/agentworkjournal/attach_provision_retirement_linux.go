package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// RetireProvisionInspection keeps unknown effects inspectable. A successful
// inspection receipt permits collecting its exact original command evidence;
// the recovery receipt is removed last so interrupted collection can resume.
func (journal *Journal) RetireProvisionInspection(ctx context.Context, terminal workexecution.Attempt) error {
	if terminal.Validate() != nil || terminal.Direction != workexecution.Recovery || terminal.FinishedAt == nil ||
		terminal.Result == nil {
		return invalid()
	}
	if err := journal.retireProvisionSource(ctx, terminal); err != nil {
		return err
	}
	if terminal.Outcome != workexecution.Succeeded {
		return nil
	}
	return journal.Retire(ctx, terminal)
}

func (journal *Journal) retireProvisionSource(ctx context.Context, terminal workexecution.Attempt) error {
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
		return nil
	}
	recovery := current.AttachProvisionRecovery
	if recovery == nil || recovery.Receipt == nil || !workexecution.SameAssignment(current.Attempt, terminal) ||
		recovery.Receipt.Result == nil || *recovery.Receipt.Result != *terminal.Result || recovery.Receipt.Outcome != terminal.Outcome ||
		!recovery.Receipt.FinishedAt.Equal(*terminal.FinishedAt) {
		return conflict()
	}
	if !recovery.Report.ExecutionStopped() {
		return nil
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
		if previous == nil || previous.Attempt.TaskID != terminal.TaskID ||
			previous.Attempt.WorkItemID != terminal.WorkItemID {
			continue
		}
		if previous.Attempt.Input != terminal.Input || previous.Attempt.Executor.HostID != terminal.Executor.HostID ||
			previous.Attempt.Number >= terminal.Number {
			return conflict()
		}
		if attemptID == recovery.Source.Forward.ID {
			if !workexecution.SameAssignment(previous.Attempt, recovery.Source.Forward) ||
				previous.AttachProvision == nil ||
				previous.AttachProvision.Authority != recovery.Source.Failure.Authority ||
				previous.AttachProvision.Completion != nil {
				return conflict()
			}
		} else {
			prior := previous.AttachProvisionRecovery
			if prior == nil || prior.Source.Forward.ID != recovery.Source.Forward.ID || prior.Source.Evidence != recovery.Source.Evidence {
				return conflict()
			}
		}
		if err := journal.removeRecord(ctx, attemptID); err != nil {
			return err
		}
	}
	return nil
}
