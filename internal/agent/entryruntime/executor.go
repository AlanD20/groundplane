package entryruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

type Exchange interface {
	ConfigurationExchange
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	EntryConfigurationCheckpoint(context.Context, *agentpb.EntryConfigurationWorkCheckpointRequest) (*agentpb.EntryConfigurationWorkCheckpointAck, error)
	CompleteEntryConfiguration(context.Context, *agentpb.EntryConfigurationWorkCompletionRequest) (*agentpb.EntryConfigurationWorkCompletionAck, error)
	FailEntryConfiguration(context.Context, *agentpb.EntryConfigurationWorkFailureRequest) (*agentpb.EntryConfigurationWorkFailureAck, error)
	RecoverEntryConfiguration(context.Context, *agentpb.EntryConfigurationWorkRecoveryReportRequest) (*agentpb.EntryConfigurationWorkRecoveryReportAck, error)
}

// Host must join started effects before returning. Inspect accounts for the
// exact input and any previously started helper; timeout is not termination.
type Host interface {
	Restore(context.Context, Input, bool) (entryexecution.RecoveryReport, error)
	Apply(context.Context, Input, entryexecution.Stage) error
	Inspect(context.Context, Input, entryexecution.Stage) (entryexecution.Observation, error)
}

type Executor struct {
	journal      *agentworkjournal.Journal
	host         Host
	materializer *materialization.Runtime
	helper       Helper
	ownership    *agentownership.Lease
}

func NewExecutor(journal *agentworkjournal.Journal, materializer *materialization.Runtime, helper Helper, ownership *agentownership.Lease) (*Executor, error) {
	if journal == nil || materializer == nil || helper == nil {
		return nil, errs.New(errs.KindInternal, "Entry execution requires its durable journal, materializer and host helper")
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
	consumers, err := NewHelperConsumers(executor.helper)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	host, err := NewHost(executor.journal, executor.ownership, configuration, consumers)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	active := *executor
	active.host = host
	return active.run(ctx, input, exchange)
}

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.Retire(ctx, terminal)
}

func (executor *Executor) run(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || input.procedure == nil || exchange == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	if input.recovery != nil {
		return executor.recover(ctx, input, exchange)
	}
	ack, err := exchange.StartAttempt(ctx, input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	input, reconcile, err := input.WithStart(ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	saved, err := executor.journal.EntryConfiguration(ctx, input.attempt, input.procedure)
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
		return executor.fail(ctx, input, exchange, errs.New(errs.KindStateConflict, "Entry configuration execution started previously but its local checkpoint is unavailable; inspection is required"))
	}
	var progress entryexecution.Progress
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
		progress, err = entryexecution.NewProgress(input.attempt, input.procedure, time.Now().UTC())
		if err != nil {
			return workexecution.Attempt{}, err
		}
	}
	for progress.Stage != entryexecution.Complete {
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
	completed, err := exchange.CompleteEntryConfiguration(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := input.completionAck(progress, completed)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.CompleteEntryConfiguration(ctx, input.attempt, input.procedure, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (executor *Executor) save(ctx context.Context, input Input, exchange Exchange, progress entryexecution.Progress) error {
	if _, err := executor.journal.PrepareEntryConfiguration(ctx, input.attempt, input.procedure, progress); err != nil {
		return err
	}
	return executor.acknowledge(ctx, input, exchange, progress)
}

func (executor *Executor) acknowledge(ctx context.Context, input Input, exchange Exchange, progress entryexecution.Progress) error {
	request, err := input.checkpointRequest(progress)
	if err != nil {
		return err
	}
	ack, err := exchange.EntryConfigurationCheckpoint(ctx, request)
	if err != nil {
		return err
	}
	checkpoint, err := input.checkpointAck(progress, ack)
	if err != nil {
		return err
	}
	return executor.journal.AcknowledgeEntryConfiguration(ctx, input.attempt, input.procedure, checkpoint)
}
