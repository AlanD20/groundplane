package attachruntime

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Execution struct {
	Progress *attachexecution.ProvisionProgress
	Failure  *workexecution.Attempt
}

// Run delivers a recorded failure instead of replaying a command. A completed
// command sequence with an unacknowledged completion keeps replaying completion,
// never converts that delivery problem into a new execution failure.
func (executor *Executor) Run(ctx context.Context, input attachexecution.ProvisionInput,
	exchange Exchange,
) (Execution, error) {
	pending, err := executor.journal.LoadAttachProvisionFailure(ctx, input.Attempt)
	if err != nil {
		return Execution{}, err
	}
	if pending != nil {
		return executor.deliverFailure(ctx, input, *pending, exchange)
	}
	progress, runErr := executor.Execute(ctx, input, exchange)
	if runErr == nil {
		return Execution{Progress: progress}, nil
	}
	local, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, saved, err := executor.journal.LoadAttachProvision(local, input)
	if err != nil {
		return Execution{}, errors.Join(runErr, err)
	}
	if saved != nil && (!saved.Acknowledged || saved.CommandResult != nil ||
		!saved.Report.Progress.InFlight && saved.Report.Progress.Completed == saved.Authority.Commands) {
		return Execution{}, runErr
	}
	reason := attachexecution.ExecutionFailed
	if ctx.Err() != nil {
		reason = attachexecution.ExecutionInterrupted
	} else if !time.Now().UTC().Before(input.Attempt.Deadline) {
		reason = attachexecution.DeadlineExpired
	}
	failure, err := executor.journal.PrepareAttachProvisionFailure(local, input.Attempt, reason)
	if err != nil {
		return Execution{}, errors.Join(runErr, err)
	}
	if ctx.Err() != nil {
		return Execution{}, runErr
	}
	result, err := executor.deliverFailure(ctx, input, failure, exchange)
	if err != nil {
		return Execution{}, errors.Join(runErr, err)
	}
	return result, nil
}

func (executor *Executor) deliverFailure(ctx context.Context, input attachexecution.ProvisionInput,
	failure agentworkjournal.AttachProvisionFailureDelivery, exchange Exchange,
) (Execution, error) {
	if failure.Receipt != nil {
		return Execution{Failure: failure.Receipt}, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return Execution{}, err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.FailAttachProvision(
		bounded,
		&agentpb.AttachProvisionFailureRequest{Attempt: identity, Reason: string(failure.Reason)},
	)
	if err != nil {
		return Execution{}, err
	}
	terminal, err := executor.journal.AcknowledgeAttachProvisionFailure(bounded, input.Attempt, ack)
	if err != nil {
		return Execution{}, err
	}
	return Execution{Failure: &terminal}, nil
}
