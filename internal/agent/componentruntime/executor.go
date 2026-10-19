package componentruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	ConfigurationExchange
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	ComponentCheckpoint(
		context.Context,
		*agentpb.ComponentWorkCheckpointRequest,
	) (*agentpb.ComponentWorkCheckpointAck, error)
	CompleteComponent(
		context.Context,
		*agentpb.ComponentWorkCompletionRequest,
	) (*agentpb.ComponentWorkCompletionAck, error)
	FailComponent(context.Context, *agentpb.ComponentWorkFailureRequest) (*agentpb.ComponentWorkFailureAck, error)
	RecoverComponent(
		context.Context,
		*agentpb.ComponentWorkRecoveryReportRequest,
	) (*agentpb.ComponentWorkRecoveryReportAck, error)
}

// Host owns exact Component runtime and configuration authority and joins any helper before
// returning. Inspect must account for previously started helpers; a deadline or
// a disconnected stream is not proof that an earlier effect stopped.
type Host interface {
	Apply(context.Context, Input, componentexecution.Stage) error
	Inspect(context.Context, Input, componentexecution.Stage) (componentexecution.Observation, error)
	Restore(context.Context, Input) (componentexecution.RecoveryReport, error)
}

type Executor struct {
	journal      *agentworkjournal.Journal
	host         Host
	materializer *materialization.Runtime
	helper       Helper
	ownership    *agentownership.Lease
}

func NewExecutor(
	journal *agentworkjournal.Journal,
	materializer *materialization.Runtime,
	helper Helper,
	ownership *agentownership.Lease,
) (*Executor, error) {
	if journal == nil || materializer == nil || helper == nil {
		return nil, errs.New(errs.KindInternal, "Component execution requires its durable journal and runtime")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &Executor{journal: journal, materializer: materializer, helper: helper, ownership: ownership}, nil
}

func (executor *Executor) Run(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	source, err := NewChannelConfigurationSource(exchange)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	configuration, err := NewConfiguration(executor.materializer, source)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	host, err := NewHost(executor.journal, executor.helper, executor.ownership, configuration)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	active := *executor
	active.host = host
	return active.run(ctx, input, exchange)
}

func (executor *Executor) run(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || input.procedure == nil || exchange == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	ack, err := exchange.StartAttempt(ctx, input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	input, reconcile, err := input.WithStart(ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if input.recovery != nil {
		return executor.runRecovery(ctx, input, exchange)
	}
	saved, err := executor.journal.Component(ctx, input.attempt, input.procedure)
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
		return executor.fail(
			ctx,
			input,
			exchange,
			errs.New(
				errs.KindStateConflict,
				"Component execution started previously but its local checkpoint is unavailable; inspection is required",
			),
		)
	}
	var progress componentexecution.Progress
	if saved != nil {
		progress = saved.Progress
		if !saved.Acknowledged {
			if err := executor.acknowledge(ctx, input, exchange, progress); err != nil {
				if progress.InFlight {
					return executor.fail(ctx, input, exchange, err)
				}
				return workexecution.Attempt{}, err
			}
		}
	} else {
		progress, err = componentexecution.NewProgress(input.attempt, input.procedure, time.Now().UTC())
		if err != nil {
			return workexecution.Attempt{}, err
		}
	}
	for progress.Stage != componentexecution.Complete {
		if err := ctx.Err(); err != nil {
			return executor.fail(ctx, input, exchange, err)
		}
		fresh := !progress.InFlight
		if fresh {
			progress, err = progress.Begin(input.attempt, input.procedure, time.Now().UTC())
			if err != nil {
				return executor.fail(ctx, input, exchange, err)
			}
			if err := executor.save(ctx, input, exchange, progress); err != nil {
				// No host effect starts without the grant. If delivery was
				// ambiguous, failure settlement uses the Controller's saved
				// checkpoint and requires inspection rather than replaying it.
				return executor.fail(ctx, input, exchange, err)
			}
		}
		// Reconnection only inspects a saved in-flight stage. It never reruns
		// Apply. Inspection remains bounded even after the effect deadline.
		inspectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		observation, inspectErr := executor.host.Inspect(inspectCtx, input, progress.Stage)
		cancel()
		if inspectErr != nil {
			return executor.fail(ctx, input, exchange, inspectErr)
		}
		if fresh && !observation.Satisfies(progress.Stage, input.procedure.Remove) {
			operationCtx, cancel := context.WithDeadline(ctx, input.attempt.Deadline)
			err = executor.host.Apply(operationCtx, input, progress.Stage)
			cancel()
			if err != nil {
				return executor.fail(ctx, input, exchange, err)
			}
			inspectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			observation, inspectErr = executor.host.Inspect(inspectCtx, input, progress.Stage)
			cancel()
			if inspectErr != nil {
				return executor.fail(ctx, input, exchange, inspectErr)
			}
		}
		progress, err = progress.Confirm(input.attempt, input.procedure, observation, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, input, exchange, err)
		}
		if err := executor.save(ctx, input, exchange, progress); err != nil {
			return workexecution.Attempt{}, err
		}
	}
	request, err := input.completionRequest(progress)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	completed, err := exchange.CompleteComponent(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := input.completionAck(progress, completed)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.CompleteComponent(ctx, input.attempt, input.procedure, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (executor *Executor) save(
	ctx context.Context,
	input Input,
	exchange Exchange,
	progress componentexecution.Progress,
) error {
	if _, err := executor.journal.PrepareComponent(ctx, input.attempt, input.procedure, progress); err != nil {
		return err
	}
	return executor.acknowledge(ctx, input, exchange, progress)
}

func (executor *Executor) acknowledge(
	ctx context.Context,
	input Input,
	exchange Exchange,
	progress componentexecution.Progress,
) error {
	request, err := input.checkpointRequest(progress)
	if err != nil {
		return err
	}
	ack, err := exchange.ComponentCheckpoint(ctx, request)
	if err != nil {
		return err
	}
	checkpoint, err := input.checkpointAck(progress, ack)
	if err != nil {
		return err
	}
	return executor.journal.AcknowledgeComponent(ctx, input.attempt, input.procedure, checkpoint)
}
