package attachruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
)

// InspectProvision never runs a hook or adapter command. A missing Docker exec
// is unobservable, not evidence that an externally authorized effect stopped.
func (host *BackingHost) InspectProvision(ctx context.Context, input attachexecution.ProvisionRecoveryInput) (attachexecution.ProvisionInspection, error) {
	if err := input.Validate(); err != nil {
		return attachexecution.ProvisionInspection{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.ProvisionInspection{}, err
	}
	report := attachexecution.ProvisionInspection{AttemptID: input.Attempt.ID, ForwardID: input.Source.Forward.ID,
		Failure: input.Source.Evidence, CommandOrdinal: uint32((input.Source.Failure.Checkpoint.Sequence - 1) / 2),
		State: attachexecution.CommandJournalLost}
	if input.Source.Failure.Checkpoint.Sequence%2 == 0 {
		// The Controller already acknowledged a completed command. It granted
		// no later command, so journal loss cannot create new effect authority.
		report.State = attachexecution.ProvisionCommandCompleted
		report.ObservedAt = time.Now().UTC()
		return report, report.Validate(input)
	}
	if input.Source.Failure.Authority.IsConsumerCommand(report.CommandOrdinal) {
		return host.inspectConsumerRecovery(ctx, input)
	}
	saved, err := host.journal.AttachProvisionRecoverySource(ctx, input)
	if err != nil {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
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
