package workdispatch

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/infra/controllerownership"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Runtime joins independent execution and maintenance loops. It never enables
// an executor: startup reconciliation must establish readiness before Run.
type Runtime struct {
	queue        *workqueue.Repository
	scheduler    *Scheduler
	controller   *ControllerExecutor
	completions  *Completions
	publications *Publications
	inputs       *Inputs
	retention    *operationretention.Collector
	diagnostics  *executiondiagnostics.Repository
	logger       *slog.Logger
}

type RuntimeConfig struct {
	Interval            time.Duration
	CollectionInterval  time.Duration
	HistoryRetention    time.Duration
	DiagnosticRetention time.Duration
	CollectionBudget    time.Duration
}

func NewRuntime(queue *workqueue.Repository, scheduler *Scheduler, controller *ControllerExecutor, completions *Completions, publications *Publications, inputs *Inputs,
	retention *operationretention.Collector, diagnostics *executiondiagnostics.Repository, logger *slog.Logger,
) (*Runtime, error) {
	if queue == nil || scheduler == nil || controller == nil || completions == nil || publications == nil || inputs == nil || retention == nil || diagnostics == nil || logger == nil ||
		scheduler.queue != queue || controller.queue != queue || completions.queue != queue || publications.queue != queue || inputs.queue != queue {
		return nil, errs.New(errs.KindInternal, "execution runtime requires one queue and complete loop owners")
	}
	return &Runtime{queue: queue, scheduler: scheduler, controller: controller, completions: completions, publications: publications, inputs: inputs, retention: retention, diagnostics: diagnostics, logger: logger}, nil
}

// Run refuses a stale or unreconciled identity. Any loop failure stops and joins
// the process's loops; it cannot silently leave assignment running without an
// executor or settlement. Durable claims and checkpoints survive shutdown.
func (runtime *Runtime) Run(ctx context.Context, ownership *controllerownership.Lease, identity workexecution.Identity, config RuntimeConfig) error {
	if err := ownership.Check(); err != nil {
		return err
	}
	if ctx == nil || identity.Validate() != nil || identity.Executor != workexecution.Controller ||
		config.Interval < 100*time.Millisecond || config.CollectionInterval < time.Second || config.HistoryRetention != workexecution.TaskHistoryRetention ||
		config.DiagnosticRetention < time.Hour || config.DiagnosticRetention > config.HistoryRetention || config.CollectionBudget <= 0 {
		return errs.New(errs.KindInternal, "execution runtime identity or intervals are invalid")
	}
	registration, err := runtime.queue.ReadExecutor(ctx, workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID})
	if err != nil {
		return err
	}
	if registration.Revision == 0 || registration.Record.Identity != identity || !registration.Record.Ready {
		return errs.New(errs.KindStateConflict, "controller execution must finish startup reconciliation before dispatch")
	}
	running, cancel := context.WithCancel(ctx)
	defer cancel()
	report := func(issue Issue) {
		runtime.logger.ErrorContext(running, "execution operation requires attention", "task_id", issue.TaskID, "work_id", issue.WorkID, "error", issue.Err)
	}
	loops := []func(context.Context) error{
		func(ctx context.Context) error { return runtime.inputs.Run(ctx, config.Interval, report) },
		func(ctx context.Context) error { return runtime.publications.Run(ctx, config.Interval, report) },
		func(ctx context.Context) error {
			return runtime.scheduler.RunPlacement(ctx, identity, config.Interval, func(sweep Sweep) {
				for _, issue := range sweep.Issues {
					report(issue)
				}
			})
		},
		func(ctx context.Context) error { return runtime.controller.Run(ctx, identity, config.Interval, report) },
		func(ctx context.Context) error { return runtime.scheduler.RunStops(ctx, config.Interval, report) },
		func(ctx context.Context) error {
			return runtime.completions.Run(ctx, config.Interval, config.HistoryRetention, report)
		},
		func(ctx context.Context) error {
			return runtime.retention.Run(ctx, config.CollectionInterval, func(taskID string, err error) { report(Issue{TaskID: taskID, Err: err}) })
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
	cancel()
	// Disable only this generation. Cancellation is not evidence that an external
	// effect stopped, so neither slots nor resource claims are released here.
	shutdown, finish := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	disableErr := runtime.disable(shutdown, identity)
	finish()
	workers.Wait()
	if disableErr != nil {
		return errs.WrapJoined(errs.KindInternal, first, disableErr)
	}
	if first != nil {
		return first
	}
	return errs.New(errs.KindInternal, "execution loop stopped without cancellation or an error")
}

func (runtime *Runtime) disable(ctx context.Context, identity workexecution.Identity) error {
	current, err := runtime.queue.ReadExecutor(ctx, workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID})
	if err != nil {
		return err
	}
	if current.Revision == 0 || current.Record.Identity != identity || !current.Record.Ready {
		return nil
	}
	change, err := workqueue.PrepareAvailability(current, identity, false, current.Record.Capacity)
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
