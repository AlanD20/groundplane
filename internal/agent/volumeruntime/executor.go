package volumeruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	VolumeCheckpoint(context.Context, *agentpb.VolumeWorkCheckpointRequest) (*agentpb.VolumeWorkCheckpointAck, error)
	CompleteVolume(context.Context, *agentpb.VolumeWorkCompletionRequest) (*agentpb.VolumeWorkCompletionAck, error)
	FailVolume(context.Context, *agentpb.VolumeWorkFailureRequest) (*agentpb.VolumeWorkFailureAck, error)
	RecoverVolume(context.Context, *agentpb.VolumeWorkRecoveryReportRequest) (*agentpb.VolumeWorkRecoveryReportAck, error)
}

// Host owns exact container/directory authority and joins any helper before
// returning. Inspect must account for previously started helpers; a deadline or
// a disconnected stream is not proof that an earlier effect stopped.
type Host interface {
	Apply(context.Context, Input, volumeexecution.Stage) error
	Inspect(context.Context, Input, volumeexecution.Stage) (volumeexecution.Observation, error)
	InspectRecovery(context.Context, Input) (volumeexecution.Observation, error)
}

type Executor struct {
	root    string
	journal *agentworkjournal.Journal
	host    Host
}

func NewExecutor(volumeRoot string, journal *agentworkjournal.Journal, host Host) (*Executor, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || journal == nil || host == nil {
		return nil, errs.New(errs.KindInternal, "Volume execution requires its host root, durable journal and runtime")
	}
	return &Executor{root: volumeRoot, journal: journal, host: host}, nil
}

func (executor *Executor) Run(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || input.procedure == nil || exchange == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	scope, err := environmentpath.Parse(executor.root, input.procedure.Candidate.AuthorizedVolumeDir)
	if err != nil || scope.EnvironmentID != input.procedure.EnvironmentId {
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
	saved, err := executor.journal.Volume(ctx, input.attempt, input.procedure.Action, input.procedure.VolumeId)
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
		return executor.fail(ctx, input, exchange, errs.New(errs.KindStateConflict, "Volume execution started previously but its local checkpoint is unavailable; inspection is required"))
	}
	var progress volumeexecution.Progress
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
		progress, err = volumeexecution.NewProgress(input.attempt, input.procedure.Action, input.procedure.VolumeId, time.Now().UTC())
		if err != nil {
			return workexecution.Attempt{}, err
		}
	}
	for progress.Stage != volumeexecution.Complete {
		if err := ctx.Err(); err != nil {
			return executor.fail(ctx, input, exchange, err)
		}
		fresh := !progress.InFlight
		if fresh {
			progress, err = progress.Begin(input.attempt, input.procedure.Action, input.procedure.VolumeId, time.Now().UTC())
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
		progress, err = progress.Confirm(input.attempt, input.procedure.Action, input.procedure.VolumeId, observation, time.Now().UTC())
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
	completed, err := exchange.CompleteVolume(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := input.completionAck(progress, completed)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeVolumeCompletion(ctx, input.attempt, input.procedure.Action, input.procedure.VolumeId, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (executor *Executor) save(ctx context.Context, input Input, exchange Exchange, progress volumeexecution.Progress) error {
	if _, err := executor.journal.PrepareVolume(ctx, input.attempt, input.procedure.Action, input.procedure.VolumeId, progress); err != nil {
		return err
	}
	return executor.acknowledge(ctx, input, exchange, progress)
}

func (executor *Executor) acknowledge(ctx context.Context, input Input, exchange Exchange, progress volumeexecution.Progress) error {
	request, err := input.checkpointRequest(progress)
	if err != nil {
		return err
	}
	ack, err := exchange.VolumeCheckpoint(ctx, request)
	if err != nil {
		return err
	}
	checkpoint, err := input.checkpointAck(progress, ack)
	if err != nil {
		return err
	}
	return executor.journal.AcknowledgeVolume(ctx, input.attempt, input.procedure.Action, input.procedure.VolumeId, checkpoint)
}
