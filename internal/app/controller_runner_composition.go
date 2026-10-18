package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"log/slog"
	"net/netip"
)

type controllerRunnerComposition struct {
	pools         config.AllocationPools
	lifecycle     *runnercapability.Executor
	maintenance   *runnercapability.Maintenance
	provisioning  *runnercapability.ProvisioningService
	mutations     *runnercapability.MutationService
	removals      *runnercapability.RemovalService
	checks        *runnercapability.ConnectionCheckService
	checkExecutor *runnercapability.ConnectionCheckExecutor
}

func newControllerRunnerComposition(
	cfg config.ControllerConfig,
	store etcd.EnvironmentBlueprintStore,
	runnerRecords *etcd.RunnerRepository,
	hierarchyRecords *etcd.HierarchyRepository,
	idempotency *etcd.IdempotencyRepository,
	intentCoordinator *requestidempotency.Coordinator,
	logger *slog.Logger,
) (*controllerRunnerComposition, error) {
	runnerPools, err := cfg.AllocationPools()
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner allocation: %w", err)
	}
	runnerTokens := runnercapability.NewTokenBroker()
	policy, err := corerunner.NewIsolationPolicy(
		runnerPools.Runner.Network, []netip.Prefix{runnerPools.Environment, runnerPools.System}, cfg.Listen.HTTP,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner network policy: %w", err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return nil, err
	}
	inputs, err := runnerwork.NewRepository(store)
	if err != nil {
		return nil, err
	}
	runnerProvisioning, err := runnercapability.NewProvisioningService(
		runnerRecords, queue, inputs, hierarchyRecords, idempotency, intentCoordinator, runnerTokens,
		runnerallocation.RunnerAllocationConfigFromPools(runnerPools), cfg.Runner.Image, policy,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner provisioning service: %w", err)
	}
	runnerMutations, err := runnercapability.NewMutationService(runnerRecords, idempotency, intentCoordinator)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner mutation service: %w", err)
	}
	runnerRemovals, err := runnercapability.NewRemovalService(runnerRecords, idempotency, intentCoordinator)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner removal service: %w", err)
	}
	diagnostics, err := executiondiagnostics.New(store, queue)
	if err != nil {
		return nil, err
	}
	lifecycle, maintenance, err := newRunnerLifecycleExecutor(logger, runnerRecords, runnerTokens, inputs, diagnostics)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner lifecycle: %w", err)
	}
	checks, checkExecutor, err := newControllerRunnerChecks(
		store,
		runnerRecords,
		idempotency,
		intentCoordinator,
		lifecycle,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner connection checks: %w", err)
	}
	return &controllerRunnerComposition{
		pools: runnerPools, lifecycle: lifecycle, maintenance: maintenance, provisioning: runnerProvisioning,
		mutations: runnerMutations, removals: runnerRemovals, checks: checks, checkExecutor: checkExecutor,
	}, nil
}
