package lifecycleruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	RecoverLifecycle(
		context.Context,
		*agentpb.ServiceLifecycleRecoveryReportRequest,
	) (*agentpb.ServiceLifecycleRecoveryReportAck, error)
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	LifecycleCheckpoint(
		context.Context,
		*agentpb.ServiceLifecycleCheckpointRequest,
	) (*agentpb.ServiceLifecycleCheckpointAck, error)
	CompleteLifecycle(
		context.Context,
		*agentpb.ServiceLifecycleCompletionRequest,
	) (*agentpb.ServiceLifecycleCompletionAck, error)
	FailLifecycle(context.Context, *agentpb.ServiceLifecycleFailureRequest) (*agentpb.ServiceLifecycleFailureAck, error)
	LifecycleHook(context.Context, *agentpb.ServiceLifecycleHookRequest) (*agentpb.ServiceLifecycleHookDelivery, error)
}

// Host must enforce captured container ownership and join started operations.
// Inspect returns nil when the stage has not been proven. An interrupted hook
// needs its durable result, never inference from a container's running state.
type Host interface {
	InspectRecovery(context.Context, Input) (lifecycleexecution.RecoveryReport, error)
	Inspect(context.Context, Input, lifecycleexecution.Stage) (*lifecycleexecution.Receipt, error)
	Apply(context.Context, Input, lifecycleexecution.Stage, Exchange) error
}

type Executor struct {
	root    string
	journal *agentworkjournal.Journal
	host    Host
}

func NewExecutor(root string, journal *agentworkjournal.Journal, host Host) (*Executor, error) {
	if environmentpath.ValidateRoot(root) != nil || journal == nil || host == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Service lifecycle execution requires its host root, durable journal and runtime",
		)
	}
	return &Executor{root: root, journal: journal, host: host}, nil
}

func (executor *Executor) Run(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || input.procedure == nil || exchange == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	for _, member := range input.procedure.Members {
		scope, err := environmentpath.Parse(executor.root, member.Artifact.AuthorizedVolumeDir)
		if err != nil || scope.EnvironmentID != input.procedure.EnvironmentId {
			return workexecution.Attempt{}, invalidInput()
		}
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
	saved, err := executor.journal.Lifecycle(ctx, input.attempt, input.procedure)
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
	var progress lifecycleexecution.Progress
	if saved != nil {
		progress = saved.Progress
		if err := progress.ValidateContinuation(input.attempt, input.procedure, input.continuation); err != nil {
			return workexecution.Attempt{}, err
		}
		if !saved.Acknowledged {
			if err := executor.acknowledge(ctx, input, exchange, progress); err != nil {
				return workexecution.Attempt{}, err
			}
		}
	} else {
		progress, err = lifecycleexecution.ResumeProgress(input.attempt, input.procedure, input.continuation, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, input, exchange)
		}
		if progress.Inherited > 0 {
			if err := executor.save(ctx, input, exchange, progress); err != nil {
				return workexecution.Attempt{}, err
			}
		}
	}
	stages, err := lifecycleexecution.Stages(input.procedure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	for !input.complete(progress) {
		if ctx.Err() != nil {
			return executor.fail(ctx, input, exchange)
		}
		stage := stages[len(progress.Completed)]
		fresh := progress.BeganAt == nil
		if fresh {
			progress, err = progress.Begin(input.attempt, input.procedure, time.Now().UTC())
			if err != nil {
				return executor.fail(ctx, input, exchange)
			}
			if err := executor.save(ctx, input, exchange, progress); err != nil {
				return executor.fail(ctx, input, exchange)
			}
		}
		inspectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		receipt, err := executor.host.Inspect(inspectCtx, input, stage)
		cancel()
		if err != nil {
			return executor.fail(ctx, input, exchange)
		}
		if receipt == nil && fresh {
			operationCtx, cancel := context.WithDeadline(ctx, input.attempt.Deadline)
			err = executor.host.Apply(operationCtx, input, stage, exchange)
			cancel()
			if err != nil {
				return executor.fail(ctx, input, exchange)
			}
			inspectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			receipt, err = executor.host.Inspect(inspectCtx, input, stage)
			cancel()
			if err != nil {
				return executor.fail(ctx, input, exchange)
			}
		}
		// A recovered in-flight stage can only be observed, never applied again.
		if receipt == nil {
			return executor.fail(ctx, input, exchange)
		}
		progress, err = progress.Confirm(input.attempt, input.procedure, *receipt, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, input, exchange)
		}
		if err := executor.save(ctx, input, exchange, progress); err != nil {
			return workexecution.Attempt{}, err
		}
	}
	request, err := input.completionRequest(progress)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	completed, err := exchange.CompleteLifecycle(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if completed != nil && completed.Stopped != nil {
		if executionplan.RejectUnknown(completed) != nil || completed.Attempt != nil || completed.Evidence != nil ||
			completed.StartedAt != nil || completed.FinishedAt != nil || completed.Outcome != "" {
			return workexecution.Attempt{}, invalidInput()
		}
		return executor.journal.AcknowledgeStoppedLifecycle(ctx, input.attempt, input.procedure, completed.Stopped)
	}
	terminal, err := input.completionAck(progress, completed)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeLifecycleCompletion(ctx, input.attempt, input.procedure, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (executor *Executor) save(
	ctx context.Context,
	input Input,
	exchange Exchange,
	progress lifecycleexecution.Progress,
) error {
	if _, err := executor.journal.PrepareLifecycle(ctx, input.attempt, input.procedure, progress); err != nil {
		return err
	}
	return executor.acknowledge(ctx, input, exchange, progress)
}

func (executor *Executor) acknowledge(
	ctx context.Context,
	input Input,
	exchange Exchange,
	progress lifecycleexecution.Progress,
) error {
	request, err := input.checkpointRequest(progress)
	if err != nil {
		return err
	}
	ack, err := exchange.LifecycleCheckpoint(ctx, request)
	if err != nil {
		return err
	}
	checkpoint, err := input.checkpointAck(progress, ack)
	if err != nil {
		return err
	}
	return executor.journal.AcknowledgeLifecycle(ctx, input.attempt, input.procedure, checkpoint)
}

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.Retire(ctx, terminal)
}
