package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
)

// PrepareAttachExec must complete before Docker ExecAttach can start this exec.
// A saved exec is never started again after a process interruption.
func (journal *Journal) PrepareAttachExec(ctx context.Context, input attachexecution.RemovalInput, execution AttachExec) error {
	if execution.validate() != nil || execution.ReadyAt != nil || execution.ExitedAt != nil ||
		!execution.CreatedAt.Before(input.Attempt.Deadline) {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if !attachExecWritable(state, input) || state.AttachFailure != nil {
		return conflict()
	}
	removal := state.AttachRemoval
	for _, previous := range removal.Executions {
		if previous.ExecID == execution.ExecID {
			// No replay permission is returned for an already persisted exec.
			return conflict()
		}
	}
	if len(removal.Executions) >= maximumAttachExecs {
		return conflict()
	}
	removal.Executions = append(removal.Executions, execution)
	return journal.writeRecord(ctx, *state)
}

// AuthorizeAttachExec records an observed readiness handshake before the caller
// sends permission. Repeated calls do not authorize a second permission write.
func (journal *Journal) AuthorizeAttachExec(ctx context.Context, input attachexecution.RemovalInput, execID string, at time.Time) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if !attachExecWritable(state, input) || state.AttachFailure != nil || !at.Before(input.Attempt.Deadline) {
		return conflict()
	}
	execution := lastAttachExec(state, execID)
	if execution == nil || execution.ReadyAt != nil || at.Before(execution.CreatedAt) {
		return conflict()
	}
	ready := at.UTC()
	execution.ReadyAt = &ready
	return journal.writeRecord(ctx, *state)
}

// ObserveAttachExecExit is also allowed after failure: it records process exit,
// not operation success, and never replaces the original failure receipt.
func (journal *Journal) ObserveAttachExecExit(ctx context.Context, input attachexecution.RemovalInput, execID string, exitCode int, at time.Time) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if !attachExecWritable(state, input) {
		return conflict()
	}
	execution := lastAttachExec(state, execID)
	if execution == nil || execution.ReadyAt == nil || at.Before(*execution.ReadyAt) {
		return conflict()
	}
	if execution.ExitedAt != nil {
		if *execution.ExitCode != exitCode {
			return conflict()
		}
		return nil
	}
	exited := at.UTC()
	execution.ExitedAt, execution.ExitCode = &exited, &exitCode
	return journal.writeRecord(ctx, *state)
}

func attachExecWritable(state *attemptRecord, input attachexecution.RemovalInput) bool {
	return matchesAttachState(state, input) && state.AttachRemoval.Acknowledged &&
		state.AttachRemoval.Report.Progress.InFlight && state.AttachRemoval.Completion == nil && state.AttachRemoval.CommandResult == nil
}

func lastAttachExec(state *attemptRecord, execID string) *AttachExec {
	executions := state.AttachRemoval.Executions
	if len(executions) == 0 || executions[len(executions)-1].ExecID != execID {
		return nil
	}
	return &executions[len(executions)-1]
}
