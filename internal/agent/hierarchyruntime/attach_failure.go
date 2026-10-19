package hierarchyruntime

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AttachExecution struct {
	Progress *attachexecution.RemovalProgress
	Failure  *workexecution.Attempt
}

// Run delivers a recorded failure instead of replaying a command. A completed
// command sequence with an unacknowledged completion keeps replaying completion,
// never converts that delivery problem into a new execution failure.
func (executor *AttachExecutor) Run(ctx context.Context, input attachexecution.RemovalInput,
	exchange AttachExchange,
) (AttachExecution, error) {
	pending, err := executor.journal.LoadAttachFailure(ctx, input.Attempt)
	if err != nil {
		return AttachExecution{}, err
	}
	if pending != nil {
		return executor.deliverAttachFailure(ctx, input, *pending, exchange)
	}
	progress, runErr := executor.Execute(ctx, input, exchange)
	if runErr == nil {
		return AttachExecution{Progress: progress}, nil
	}
	local, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, saved, err := executor.journal.LoadAttachRemoval(local, input)
	if err != nil {
		return AttachExecution{}, errors.Join(runErr, err)
	}
	if saved != nil && (!saved.Acknowledged || saved.CommandResult != nil ||
		!saved.Report.Progress.InFlight && saved.Report.Progress.Completed == saved.Authority.Commands) {
		return AttachExecution{}, runErr
	}
	reason := attachexecution.ExecutionFailed
	if ctx.Err() != nil {
		reason = attachexecution.ExecutionInterrupted
	} else if !time.Now().UTC().Before(input.Attempt.Deadline) {
		reason = attachexecution.DeadlineExpired
	}
	failure, err := executor.journal.PrepareAttachFailure(local, input.Attempt, reason)
	if err != nil {
		return AttachExecution{}, errors.Join(runErr, err)
	}
	if ctx.Err() != nil {
		return AttachExecution{}, runErr
	}
	result, err := executor.deliverAttachFailure(ctx, input, failure, exchange)
	if err != nil {
		return AttachExecution{}, errors.Join(runErr, err)
	}
	return result, nil
}

func (executor *AttachExecutor) deliverAttachFailure(ctx context.Context, input attachexecution.RemovalInput,
	failure agentworkjournal.AttachFailureDelivery, exchange AttachExchange,
) (AttachExecution, error) {
	if failure.Receipt != nil {
		return AttachExecution{Failure: failure.Receipt}, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return AttachExecution{}, err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.FailAttachRemoval(
		bounded,
		&agentpb.AttachRemovalFailureRequest{Attempt: identity, Reason: string(failure.Reason)},
	)
	if err != nil {
		return AttachExecution{}, err
	}
	terminal, err := executor.journal.AcknowledgeAttachFailure(bounded, input.Attempt, ack)
	if err != nil {
		return AttachExecution{}, err
	}
	return AttachExecution{Failure: &terminal}, nil
}
