package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
)

// PrepareAttachProvisionExec must complete before Docker ExecAttach can start this exec.
// A saved exec is never started again after a process interruption.
func (journal *Journal) PrepareAttachProvisionExec(
	ctx context.Context,
	input attachexecution.ProvisionInput,
	execution AttachExec,
) error {
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
	if !attachProvisionExecWritable(state, input) || state.AttachProvisionFailure != nil {
		return conflict()
	}
	provision := state.AttachProvision
	for _, previous := range provision.Executions {
		if previous.ExecID == execution.ExecID {
			// No replay permission is returned for an already persisted exec.
			return conflict()
		}
	}
	if len(provision.Executions) >= maximumAttachExecs {
		return conflict()
	}
	provision.Executions = append(provision.Executions, execution)
	return journal.writeRecord(ctx, *state)
}

// AuthorizeAttachProvisionExec records an observed readiness handshake before the caller
// sends permission. Repeated calls do not authorize a second permission write.
func (journal *Journal) AuthorizeAttachProvisionExec(
	ctx context.Context,
	input attachexecution.ProvisionInput,
	execID string,
	at time.Time,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if !attachProvisionExecWritable(state, input) || state.AttachProvisionFailure != nil ||
		!at.Before(input.Attempt.Deadline) {
		return conflict()
	}
	execution := lastAttachProvisionExec(state, execID)
	if execution == nil || execution.ReadyAt != nil || at.Before(execution.CreatedAt) {
		return conflict()
	}
	ready := at.UTC()
	execution.ReadyAt = &ready
	return journal.writeRecord(ctx, *state)
}

// ObserveAttachProvisionExecExit records process exit, not operation success.
// A later inspection can record the exit of an interrupted command without
// granting permission to start it again.
func (journal *Journal) ObserveAttachProvisionExecExit(
	ctx context.Context,
	input attachexecution.ProvisionInput,
	execID string,
	exitCode int,
	at time.Time,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if !attachProvisionExecWritable(state, input) {
		return conflict()
	}
	execution := lastAttachProvisionExec(state, execID)
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

func attachProvisionExecWritable(state *attemptRecord, input attachexecution.ProvisionInput) bool {
	return matchesAttachProvisionState(state, input) && state.AttachProvision.Acknowledged &&
		state.AttachProvision.Report.Progress.InFlight && state.AttachProvision.CommandResult == nil && state.AttachProvision.Completion == nil
}

func lastAttachProvisionExec(state *attemptRecord, execID string) *AttachExec {
	executions := state.AttachProvision.Executions
	if len(executions) == 0 || executions[len(executions)-1].ExecID != execID {
		return nil
	}
	return &executions[len(executions)-1]
}
