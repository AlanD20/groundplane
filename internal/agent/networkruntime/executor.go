package networkruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	Start(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	NetworkCheckpoint(context.Context, *agentpb.NetworkWorkCheckpointRequest) (*agentpb.NetworkWorkCheckpointAck, error)
	CompleteNetwork(context.Context, *agentpb.NetworkWorkCompletionRequest) (*agentpb.NetworkWorkCompletionAck, error)
	FailNetwork(context.Context, *agentpb.NetworkWorkFailureRequest) (*agentpb.NetworkWorkFailureAck, error)
	RecoverNetwork(context.Context, *agentpb.NetworkWorkRecoveryReportRequest) (*agentpb.NetworkWorkRecoveryReportAck, error)
}

// Host owns exact bridge authority and joins any helper before
// returning. Inspect must account for previously started helpers; a deadline or
// a disconnected stream is not proof that an earlier effect stopped.
type Host interface {
	Apply(context.Context, Input, networkexecution.Stage) error
	Inspect(context.Context, Input, networkexecution.Stage) (networkexecution.Observation, error)
	InspectRecovery(context.Context, Input) (networkexecution.Observation, error)
}

type Executor struct {
	journal *agentworkjournal.Journal
	host    Host
}

func NewExecutor(journal *agentworkjournal.Journal, host Host) (*Executor, error) {
	if journal == nil || host == nil {
		return nil, errs.New(errs.KindInternal, "Network execution requires its durable journal and runtime")
	}
	return &Executor{journal: journal, host: host}, nil
}

func (executor *Executor) Run(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || input.procedure == nil || exchange == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	ack, err := exchange.Start(ctx, input.attempt)
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
	saved, err := executor.journal.Network(ctx, input.attempt, input.procedure.Action, input.procedure.NetworkId)
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
		return executor.fail(ctx, input, exchange, errs.New(errs.KindStateConflict, "Network execution started previously but its local checkpoint is unavailable; inspection is required"))
	}
	var progress networkexecution.Progress
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
		progress, err = networkexecution.NewProgress(input.attempt, input.procedure.Action, input.procedure.NetworkId, time.Now().UTC())
		if err != nil {
			return workexecution.Attempt{}, err
		}
	}
	for progress.Stage != networkexecution.Complete {
		if err := ctx.Err(); err != nil {
			return executor.fail(ctx, input, exchange, err)
		}
		fresh := !progress.InFlight
		if fresh {
			progress, err = progress.Begin(input.attempt, input.procedure.Action, input.procedure.NetworkId, time.Now().UTC())
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
		if fresh && !observation.Satisfies(progress.Stage) {
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
		progress, err = progress.Confirm(input.attempt, input.procedure.Action, input.procedure.NetworkId, observation, time.Now().UTC())
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
	completed, err := exchange.CompleteNetwork(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := input.completionAck(progress, completed)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeNetworkCompletion(ctx, input.attempt, input.procedure.Action, input.procedure.NetworkId, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (executor *Executor) save(ctx context.Context, input Input, exchange Exchange, progress networkexecution.Progress) error {
	if _, err := executor.journal.PrepareNetwork(ctx, input.attempt, input.procedure.Action, input.procedure.NetworkId, progress); err != nil {
		return err
	}
	return executor.acknowledge(ctx, input, exchange, progress)
}

func (executor *Executor) acknowledge(ctx context.Context, input Input, exchange Exchange, progress networkexecution.Progress) error {
	request, err := input.checkpointRequest(progress)
	if err != nil {
		return err
	}
	ack, err := exchange.NetworkCheckpoint(ctx, request)
	if err != nil {
		return err
	}
	checkpoint, err := input.checkpointAck(progress, ack)
	if err != nil {
		return err
	}
	return executor.journal.AcknowledgeNetwork(ctx, input.attempt, input.procedure.Action, input.procedure.NetworkId, checkpoint)
}
