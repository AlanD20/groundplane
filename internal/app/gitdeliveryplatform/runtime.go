package gitdeliveryplatform

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/AlanD20/groundplane/internal/app/runneraccessplatform"
	"github.com/AlanD20/groundplane/internal/common/config"
	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/gitreleasebridge"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/runner"
	"github.com/AlanD20/groundplane/internal/controller/runneridentity"
	"github.com/AlanD20/groundplane/internal/controller/runnermigrationoperator"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RuntimeDependencies struct {
	Config      config.ControllerConfig
	Store       etcd.EnvironmentBlueprintStore
	Cipher      githubconnection.Cipher
	Intents     *requestidempotency.Coordinator
	Replays     *etcd.IdempotencyRepository
	Accepted    *acceptedaction.Submitter
	Images      *imagedelivery.Service
	Releases    *releaseoperation.Service
	Ledger      *etcd.ReleaseLedger
	Runner      *runner.Executor
	Maintenance *runner.Maintenance
	Logger      *slog.Logger
	Interval    time.Duration
}

type Runtime struct {
	Operator            *Operator
	Migration           handlers.RunnerScopedMigrationOperator
	CLIGrants           handlers.RunnerCLIGrantOperator
	RunnerMigrationWork *runnermigrationoperator.Work
	RunnerAPI           *handlers.RunnerListener
	Registry            *handlers.RunnerListener
	Webhooks            *handlers.RunnerListener
	httpReady           atomic.Bool
	start               chan struct{}
	interval            time.Duration
	logger              *slog.Logger
}

func NewRuntime(ctx context.Context, dependencies RuntimeDependencies) (*Runtime, error) {
	queue, err := workqueue.NewRepository(dependencies.Store)
	if err != nil {
		return nil, err
	}
	listen := dependencies.Config.Listen
	runtime := &Runtime{start: make(chan struct{}), interval: dependencies.Interval, logger: dependencies.Logger}
	var authorities []RunnerAuthority
	if listen.ScopedRunnerRegistry != "" {
		authorities = append(authorities, runneraccessplatform.NewGitHubAuthority(
			dependencies.Store, listen.ScopedRunnerHTTP, listen.ScopedRunnerRegistry, runtime.httpReady.Load,
		))
	}
	bridge := gitreleasebridge.New(
		dependencies.Images,
		dependencies.Releases,
		queue,
		dependencies.Ledger,
		dependencies.Store,
	)
	operator, err := New(
		dependencies.Store,
		dependencies.Cipher,
		dependencies.Intents,
		dependencies.Replays,
		bridge,
		authorities...)
	if err != nil {
		return nil, err
	}
	runtime.Operator = operator
	if listen.GitHubWebhooks != "" {
		runtime.Webhooks = &handlers.RunnerListener{Address: listen.GitHubWebhooks, Handler: operator.GitHubWebhook()}
	}
	if listen.ScopedRunnerHTTP != "" {
		access, err := runneraccessplatform.New(
			dependencies.Store,
			queue,
			dependencies.Releases,
			dependencies.Images,
			dependencies.Logger,
		)
		if err != nil {
			return nil, err
		}
		runtime.RunnerAPI, runtime.Registry, err = access.GitHubListeners(
			ctx, dependencies.Store, operator, listen.ScopedRunnerHTTP, listen.ScopedRunnerRegistry,
		)
		if err != nil {
			return nil, err
		}
		if runtime.Registry != nil {
			runtime.CLIGrants = runneridentity.NewGrantOperator(
				access.Grants,
				dependencies.Intents,
				dependencies.Replays,
			)
		}
	}
	if runtime.Registry != nil {
		if err := runtime.configureMigration(dependencies, queue); err != nil {
			return nil, err
		}
	}
	return runtime, nil
}

func (runtime *Runtime) configureMigration(dependencies RuntimeDependencies, queue *workqueue.Repository) error {
	pools, err := dependencies.Config.AllocationPools()
	if err != nil {
		return err
	}
	policy, err := corerunner.NewScopedIsolationPolicy(
		pools.Runner.Network, []netip.Prefix{pools.Environment, pools.System}, dependencies.Config.InternalAPI(),
		runtime.RunnerAPI.Address, runtime.Registry.Address,
	)
	if err != nil {
		return err
	}
	migration, err := runneraccessplatform.NewMigration(dependencies.Store, dependencies.Cipher,
		commandrunner.New(dependencies.Logger), dependencies.Runner)
	if err != nil {
		return err
	}
	readiness := func(ctx context.Context) error {
		if !runtime.httpReady.Load() {
			return errs.New(errs.KindStateConflict, "Scoped Runner API and registry are unavailable")
		}
		return ctx.Err()
	}
	operator, err := runnermigrationoperator.New(
		runnerscoped.NewRepository(
			dependencies.Store,
		),
		runners.NewReader(dependencies.Store),
		migration,
		runtime.Operator,
		readiness,
		policy,
		dependencies.Config.Runner.ScopedImage,
		dependencies.Intents,
		dependencies.Replays,
		dependencies.Accepted,
	)
	if err != nil {
		return err
	}
	runtime.RunnerMigrationWork, err = runnermigrationoperator.NewWork(operator, queue)
	if err == nil {
		runtime.Migration = operator
		dependencies.Maintenance.ConfigureScopedMigration(migration)
	}
	return err
}

func (runtime *Runtime) MarkHTTPReady() {
	if runtime.httpReady.CompareAndSwap(false, true) {
		close(runtime.start)
	}
}

func (runtime *Runtime) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-runtime.start:
	}
	err := runtime.Operator.Run(ctx, runtime.interval, runtime.logger)
	if errors.Is(err, ctx.Err()) {
		return nil
	}
	return err
}
