package removalruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	RemovalRecoveryCheckpoint(
		context.Context,
		*agentpb.ServiceRemovalRecoveryCheckpointRequest,
	) (*agentpb.ServiceRemovalRecoveryCheckpointAck, error)
	CompleteRemovalRecovery(
		context.Context,
		*agentpb.ServiceRemovalRecoveryCompletionRequest,
	) (*agentpb.ServiceRemovalRecoveryCompletionAck, error)
	FailRemovalRecovery(
		context.Context,
		*agentpb.ServiceRemovalRecoveryFailureRequest,
	) (*agentpb.ServiceRemovalRecoveryFailureAck, error)
}

// Host verifies captured ownership and joins every launched helper. Inspect
// returns nil when it cannot prove the requested recovery phase. Apply accepts
// only an acknowledged grant, including the initial executor-quiescence phase.
type Host interface {
	Inspect(context.Context, Input, removalexecution.Progress) (*workexecution.Input, error)
	Apply(context.Context, Input, removalexecution.Progress, workexecution.Checkpoint) error
}

type Executor struct {
	journal *agentworkjournal.Journal
	host    Host
}

func NewExecutor(journal *agentworkjournal.Journal, host Host) (*Executor, error) {
	if journal == nil || host == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Service removal recovery requires its durable journal and host executor",
		)
	}
	return &Executor{journal: journal, host: host}, nil
}

func (executor *Executor) Run(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || input.procedure == nil || exchange == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	start, err := exchange.StartAttempt(ctx, input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	input, reconcile, err := input.WithStart(start)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	saved, err := executor.journal.RemovalRecovery(ctx, input.attempt, input.procedure, input.capture)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved != nil && saved.Completion != nil {
		return *saved.Completion, nil
	}
	if saved != nil && saved.Failure != nil {
		return executor.deliverFailure(ctx, input, exchange, saved.Failure)
	}
	if saved == nil && reconcile {
		return executor.fail(ctx, input, exchange)
	}
	var progress removalexecution.Progress
	if saved == nil {
		progress, err = removalexecution.NewProgress(input.attempt, input.procedure, input.capture, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, input, exchange)
		}
	} else {
		progress = saved.Progress
		if !saved.Acknowledged {
			if _, err := executor.acknowledge(ctx, input, exchange, progress); err != nil {
				return workexecution.Attempt{}, err
			}
		}
	}
	for progress.Phase != removalexecution.Complete {
		if ctx.Err() != nil {
			return executor.fail(ctx, input, exchange)
		}
		fresh := !progress.InFlight
		var grant workexecution.Checkpoint
		if fresh {
			progress, err = progress.Begin(input.attempt, input.procedure, input.capture, time.Now().UTC())
			if err != nil {
				return executor.fail(ctx, input, exchange)
			}
			grant, err = executor.save(ctx, input, exchange, progress)
			if err != nil {
				return workexecution.Attempt{}, err
			}
		}
		inspectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		evidence, err := executor.host.Inspect(inspectCtx, input, progress)
		cancel()
		if err != nil {
			return executor.fail(ctx, input, exchange)
		}
		if evidence == nil && fresh {
			effectCtx, cancel := context.WithDeadline(ctx, input.attempt.Deadline)
			err = executor.host.Apply(effectCtx, input, progress, grant)
			cancel()
			if err != nil {
				return executor.fail(ctx, input, exchange)
			}
			inspectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			evidence, err = executor.host.Inspect(inspectCtx, input, progress)
			cancel()
			if err != nil {
				return executor.fail(ctx, input, exchange)
			}
		}
		if evidence == nil {
			return executor.fail(ctx, input, exchange)
		}
		progress, err = progress.Confirm(input.attempt, input.procedure, input.capture, *evidence, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, input, exchange)
		}
		if _, err := executor.save(ctx, input, exchange, progress); err != nil {
			return workexecution.Attempt{}, err
		}
	}
	ack, err := executor.acknowledge(ctx, input, exchange, progress)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	request, err := input.CompletionRequest(progress, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	response, err := exchange.CompleteRemovalRecovery(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := input.AcknowledgeCompletion(progress, response)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.CompleteRemovalRecovery(ctx, input.attempt, input.procedure, input.capture, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (executor *Executor) save(
	ctx context.Context,
	input Input,
	exchange Exchange,
	progress removalexecution.Progress,
) (workexecution.Checkpoint, error) {
	if _, err := executor.journal.PrepareRemovalRecovery(ctx, input.attempt, input.procedure, input.capture, progress); err != nil {
		return workexecution.Checkpoint{}, err
	}
	ack, err := executor.acknowledge(ctx, input, exchange, progress)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	return input.Acknowledged(progress, ack)
}

func (executor *Executor) acknowledge(
	ctx context.Context,
	input Input,
	exchange Exchange,
	progress removalexecution.Progress,
) (*agentpb.ServiceRemovalRecoveryCheckpointAck, error) {
	request, err := input.CheckpointRequest(progress)
	if err != nil {
		return nil, err
	}
	ack, err := exchange.RemovalRecoveryCheckpoint(ctx, request)
	if err != nil {
		return nil, err
	}
	checkpoint, err := input.Acknowledged(progress, ack)
	if err != nil {
		return nil, err
	}
	if err := executor.journal.AcknowledgeRemovalRecovery(ctx, input.attempt, input.procedure, input.capture, checkpoint); err != nil {
		return nil, err
	}
	return ack, nil
}

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.Retire(ctx, terminal)
}
