package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InspectRemoval never invokes an adapter or hook, changes a container, or
// infers that a missing journal means the command did not execute.
func (host *BackingHost) InspectRemoval(
	ctx context.Context,
	input attachexecution.RecoveryInput,
) (attachexecution.RemovalInspection, error) {
	if err := input.Validate(); err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	report := attachexecution.RemovalInspection{
		AttemptID: input.Attempt.ID, ForwardID: input.Source.Forward.ID, Failure: input.Source.Evidence,
		CommandOrdinal: uint32(input.Source.Failure.Checkpoint.Sequence / 2), State: attachexecution.CommandJournalLost,
	}
	if input.RequiresConsumer() {
		return report, errs.New(errs.KindInternal, "consumer inspection must use the Attach runtime host")
	}
	saved, err := host.journal.AttachRecoverySource(ctx, input.Attempt, input.Source)
	if err != nil {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		// The public report contains no local filesystem or private error text.
		report.ObservedAt = time.Now().UTC()
		return report, report.Validate(input)
	}
	report.State = attachexecution.CommandNotCreated
	if len(saved.Executions) > 0 {
		execution := saved.Executions[len(saved.Executions)-1]
		report.PreparedCommands = uint32(len(saved.Executions))
		report.ContainerID, report.ExecID = execution.ContainerID, execution.ExecID
		switch {
		case execution.ReadyAt == nil:
			report.State = attachexecution.CommandNotPermitted
		case execution.ExitedAt != nil:
			report.State = attachexecution.CommandExited
			exitCode := *execution.ExitCode
			report.ExitCode = &exitCode
		default:
			observed, inspectErr := host.commands.Inspect(ctx, execution.ContainerID, execution.ExecID)
			if ctx.Err() != nil {
				return report, ctx.Err()
			}
			switch {
			case inspectErr != nil:
				report.State = attachexecution.CommandUnobservable
			case observed.Running:
				report.State = attachexecution.CommandRunning
			default:
				report.State = attachexecution.CommandExited
				report.ExitCode = &observed.ExitCode
			}
		}
	}
	if err := host.ownership.Check(); err != nil {
		return report, err
	}
	report.ObservedAt = time.Now().UTC()
	return report, report.Validate(input)
}
