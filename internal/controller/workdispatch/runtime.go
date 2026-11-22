package workdispatch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Runtime joins independent execution and maintenance loops. It never enables
// an executor: startup reconciliation must establish readiness before Run.
type Runtime struct {
	queue        *workqueue.Repository
	health       storageHealth
	forward      *ForwardGate
	scheduler    *Scheduler
	controller   *ControllerExecutor
	completions  *Completions
	publications *Publications
	inputs       *Inputs
	retention    retentionLoop
	diagnostics  *executiondiagnostics.Repository
	logger       *slog.Logger
}

type storageHealth interface {
	Health(context.Context) error
}

type retentionLoop interface {
	Run(context.Context, time.Duration, func(string, error)) error
}

type executionOwnership interface {
	Check() error
}

// Coordination is latency-sensitive queue processing, separate from the
// operator-configured periodic scheduler interval.
const (
	coordinationInterval     = time.Second
	maximumCoordinationBurst = 32
)

type RuntimeConfig struct {
	CollectionInterval  time.Duration
	HistoryRetention    time.Duration
	DiagnosticRetention time.Duration
	CollectionBudget    time.Duration
}

func NewRuntime(
	queue *workqueue.Repository,
	health storageHealth,
	forward *ForwardGate,
	scheduler *Scheduler,
	controller *ControllerExecutor,
	completions *Completions,
	publications *Publications,
	inputs *Inputs,
	retention retentionLoop,
	diagnostics *executiondiagnostics.Repository,
	logger *slog.Logger,
) (*Runtime, error) {
	if queue == nil || health == nil || forward == nil || scheduler == nil || controller == nil || completions == nil ||
		publications == nil ||
		inputs == nil ||
		retention == nil ||
		diagnostics == nil ||
		logger == nil ||
		scheduler.forward != forward ||
		controller.forward != forward ||
		scheduler.queue != queue ||
		controller.queue != queue ||
		completions.queue != queue ||
		publications.queue != queue ||
		inputs.queue != queue {
		return nil, errs.New(errs.KindInternal, "execution runtime requires one queue and complete loop owners")
	}
	return &Runtime{
		queue:        queue,
		health:       health,
		forward:      forward,
		scheduler:    scheduler,
		controller:   controller,
		completions:  completions,
		publications: publications,
		inputs:       inputs,
		retention:    retention,
		diagnostics:  diagnostics,
		logger:       logger,
	}, nil
}

// Run refuses a stale or unreconciled identity. A typed storage outage pauses
// forward work and reconciles under this process's retained lease and identity;
// permanent failures stop and join every loop. Durable claims and checkpoints
// survive either path.
func (runtime *Runtime) Run(
	ctx context.Context,
	ownership executionOwnership,
	identity workexecution.Identity,
	config RuntimeConfig,
) error {
	if err := ownership.Check(); err != nil {
		return err
	}
	if ctx == nil || identity.Validate() != nil || identity.Executor != workexecution.Controller ||
		config.CollectionInterval < time.Second || config.HistoryRetention != workexecution.TaskHistoryRetention ||
		config.DiagnosticRetention < time.Hour || config.DiagnosticRetention > config.HistoryRetention || config.CollectionBudget <= 0 {
		return errs.New(errs.KindInternal, "execution runtime identity or intervals are invalid")
	}
	for {
		failure, pause, recoverable, cycleErr := runtime.runCycle(ctx, identity, config)
		if cycleErr != nil {
			failure = errs.WrapJoined(errs.KindInternal, failure, cycleErr)
		}
		if cycleErr == nil && ctx.Err() == nil && recoverable && pause != nil {
			runtime.logger.ErrorContext(
				ctx,
				"execution storage is unavailable; forward work is paused",
				"error",
				failure,
			)
			issues, recoveryErr := runtime.waitForStorageRecovery(ctx, ownership, identity)
			for _, issue := range issues {
				runtime.logger.ErrorContext(
					ctx,
					"storage recovery retained unresolved execution",
					"task_id",
					issue.TaskID,
					"work_id",
					issue.WorkID,
					"error",
					issue.Err,
				)
			}
			if recoveryErr == nil {
				if err := pause.Resume(); err == nil {
					continue
				} else {
					failure = errs.WrapJoined(errs.KindInternal, failure, err)
				}
			} else {
				failure = errs.WrapJoined(errs.KindStateConflict, failure, recoveryErr)
			}
		}
		shutdown, finish := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		disableErr := runtime.disable(shutdown, identity)
		finish()
		if disableErr != nil {
			return errs.WrapJoined(errs.KindInternal, failure, disableErr)
		}
		if failure != nil {
			return failure
		}
		return errs.New(errs.KindInternal, "execution loop stopped without cancellation or an error")
	}
}

func (runtime *Runtime) runCycle(
	ctx context.Context,
	identity workexecution.Identity,
	config RuntimeConfig,
) (error, *ForwardPause, bool, error) {
	registration, err := runtime.queue.ReadExecutor(
		ctx,
		workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID},
	)
	if err != nil {
		pause, pauseErr := runtime.pauseForward(ctx)
		return err, pause, storageUnavailable(err), pauseErr
	}
	if registration.Revision == 0 || registration.Record.Identity != identity || !registration.Record.Ready {
		pause, pauseErr := runtime.pauseForward(ctx)
		return errs.New(
			errs.KindStateConflict,
			"controller execution must finish startup reconciliation before dispatch",
		), pause, false, pauseErr
	}
	running, cancel := context.WithCancel(ctx)
	report := func(issue Issue) {
		runtime.logger.ErrorContext(
			running,
			"execution reconciliation failed",
			"task_id",
			issue.TaskID,
			"work_id",
			issue.WorkID,
			"error",
			issue.Err,
		)
	}
	loops := []func(context.Context) error{
		func(ctx context.Context) error { return runtime.inputs.Run(ctx, coordinationInterval, report) },
		func(ctx context.Context) error { return runtime.publications.Run(ctx, coordinationInterval, report) },
		func(ctx context.Context) error {
			return runtime.scheduler.RunPlacement(ctx, identity, coordinationInterval, func(sweep Sweep) {
				for _, issue := range sweep.Issues {
					report(issue)
				}
			})
		},
		func(ctx context.Context) error {
			return runtime.controller.Run(ctx, identity, coordinationInterval, report)
		},
		func(ctx context.Context) error { return runtime.scheduler.RunStops(ctx, coordinationInterval, report) },
		func(ctx context.Context) error {
			return runtime.completions.Run(ctx, coordinationInterval, config.HistoryRetention, report)
		},
		func(ctx context.Context) error {
			return runtime.retention.Run(
				ctx,
				config.CollectionInterval,
				func(taskID string, err error) { report(Issue{TaskID: taskID, Err: err}) },
			)
		},
		func(ctx context.Context) error { return runtime.expireDiagnostics(ctx, config, report) },
	}
	results := make(chan error, len(loops))
	var workers sync.WaitGroup
	for _, loop := range loops {
		workers.Add(1)
		go func(loop func(context.Context) error) {
			defer workers.Done()
			results <- loop(running)
		}(loop)
	}
	first := <-results
	pause, pauseErr := runtime.forward.Pause()
	cancel()
	workers.Wait()
	close(results)
	failure := first
	recoverable := recoverableStorageFailure(first)
	for loopErr := range results {
		if loopErr == nil || cancellationOnly(loopErr) {
			continue
		}
		failure = errs.WrapJoined(errs.KindInternal, failure, loopErr)
		if !recoverableStorageFailure(loopErr) {
			recoverable = false
		}
	}
	if pauseErr != nil {
		return failure, nil, false, pauseErr
	}
	drain, finish := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	drainErr := pause.Wait(drain)
	finish()
	if drainErr != nil {
		return failure, pause, false, drainErr
	}
	return failure, pause, recoverable, nil
}

func (runtime *Runtime) pauseForward(ctx context.Context) (*ForwardPause, error) {
	pause, err := runtime.forward.Pause()
	if err != nil {
		return nil, err
	}
	drain, finish := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	err = pause.Wait(drain)
	finish()
	return pause, err
}

func storageUnavailable(err error) bool {
	return errors.Is(err, errs.New(errs.KindStorageUnavailable, ""))
}

// recoverableStorageFailure accepts nested Internal wrappers because storage
// adapters may add private context, but rejects an error graph that also
// carries a permanent Groundplane failure. Cancellation from sibling loops is
// ignored separately; it cannot turn a permanent failure into an outage.
func recoverableStorageFailure(err error) bool {
	allowed, storage := storageRecoveryCauses(err)
	return allowed && storage
}

func cancellationOnly(err error) bool {
	allowed, storage := storageRecoveryCauses(err)
	return allowed && !storage
}

func storageRecoveryCauses(err error) (bool, bool) {
	if err == nil || err == context.Canceled {
		return true, false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		allowed := true
		var storage bool
		for _, cause := range joined.Unwrap() {
			causeAllowed, causeStorage := storageRecoveryCauses(cause)
			allowed = allowed && causeAllowed
			storage = storage || causeStorage
		}
		return allowed, storage
	}
	if typed, ok := err.(*errs.Error); ok {
		switch typed.Kind() {
		case errs.KindStorageUnavailable:
			return true, true
		case errs.KindInternal:
			cause := errors.Unwrap(typed)
			if cause == nil {
				return false, false
			}
			return storageRecoveryCauses(cause)
		default:
			return false, false
		}
	}
	if cause := errors.Unwrap(err); cause != nil {
		return storageRecoveryCauses(cause)
	}
	return false, false
}

func (runtime *Runtime) disable(ctx context.Context, identity workexecution.Identity) error {
	current, err := runtime.queue.ReadExecutor(
		ctx,
		workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID},
	)
	if err != nil {
		return err
	}
	if current.Revision == 0 || current.Record.Identity != identity || !current.Record.Ready {
		return nil
	}
	change, err := workqueue.PrepareAvailability(
		current,
		identity,
		false,
		current.Record.Capacity,
		current.Record.UnavailableOperations,
	)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := runtime.queue.Commit(ctx, change)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "executor availability changed during shutdown")
	}
	return nil
}
