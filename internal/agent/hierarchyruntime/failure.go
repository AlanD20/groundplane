package hierarchyruntime

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EnvironmentExecution struct {
	Progress *hierarchyexecution.EnvironmentProgress
	Failure  *workexecution.Attempt
}

// RunEnvironment retries delivery of a recorded outcome, never execution of a
// recorded failure. Errors after the final absence report resume completion.
func (executor *Executor) RunEnvironment(
	ctx context.Context,
	input hierarchyexecution.EnvironmentInput,
	exchange Exchange,
) (EnvironmentExecution, error) {
	pending, err := executor.journal.LoadEnvironmentFailure(ctx, input.Attempt)
	if err != nil {
		return EnvironmentExecution{}, err
	}
	if pending != nil {
		return executor.deliverFailure(ctx, input, *pending, exchange)
	}
	progress, runErr := executor.ExecuteEnvironment(ctx, input, exchange)
	if runErr == nil {
		return EnvironmentExecution{Progress: progress}, nil
	}
	// Cancellation must still leave durable local intent for the next stream.
	local, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, saved, err := executor.journal.LoadEnvironmentRemoval(local, input)
	if err != nil {
		return EnvironmentExecution{}, errors.Join(runErr, err)
	}
	if saved != nil && saved.Report.Progress.Phase == hierarchyexecution.EnvironmentRemoved {
		return EnvironmentExecution{}, runErr
	}
	reason := hierarchyexecution.ExecutionFailed
	if ctx.Err() != nil {
		reason = hierarchyexecution.ExecutionInterrupted
	} else if !time.Now().UTC().Before(input.Attempt.Deadline) {
		reason = hierarchyexecution.DeadlineExpired
	}
	failure, err := executor.journal.PrepareEnvironmentFailure(local, input.Attempt, reason)
	if err != nil {
		return EnvironmentExecution{}, errors.Join(runErr, err)
	}
	if ctx.Err() != nil {
		return EnvironmentExecution{}, runErr
	}
	result, err := executor.deliverFailure(ctx, input, failure, exchange)
	if err != nil {
		return EnvironmentExecution{}, errors.Join(runErr, err)
	}
	return result, nil
}

func (executor *Executor) deliverFailure(ctx context.Context, input hierarchyexecution.EnvironmentInput,
	failure agentworkjournal.EnvironmentFailureDelivery, exchange Exchange,
) (EnvironmentExecution, error) {
	if failure.Receipt != nil {
		return EnvironmentExecution{Failure: failure.Receipt}, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return EnvironmentExecution{}, err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.FailEnvironmentRemoval(
		bounded,
		&agentpb.EnvironmentRemovalFailureRequest{Attempt: identity, Reason: string(failure.Reason)},
	)
	if err != nil {
		return EnvironmentExecution{}, err
	}
	terminal, err := executor.journal.AcknowledgeEnvironmentFailure(bounded, input.Attempt, ack)
	if err != nil {
		return EnvironmentExecution{}, err
	}
	return EnvironmentExecution{Failure: &terminal}, nil
}
