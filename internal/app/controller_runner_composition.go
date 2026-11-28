package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/app/runnerassembly"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"log/slog"
	"net/netip"
)

type controllerRunnerComposition struct {
	software         *runnercapability.SoftwareService
	softwareExecutor *runnercapability.SoftwareExecutor

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
	accepted *acceptedaction.Submitter,
) (*controllerRunnerComposition, error) {
	runnerPools, err := cfg.AllocationPools()
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner allocation: %w", err)
	}
	runnerTokens := runnercapability.NewTokenBroker()
	runnerPolicy := corerunner.NewIsolationPolicy
	runnerImage := cfg.Runner.Image
	if cfg.Listen.ScopedRunnerRegistry != "" {
		runnerPolicy = corerunner.NewRunnerRegistrationPolicy
		runnerImage = cfg.Runner.ScopedImage
	}
	policy, err := runnerPolicy(
		runnerPools.Runner.Network, []netip.Prefix{runnerPools.Environment, runnerPools.System}, cfg.InternalAPI(),
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
		runnerallocation.RunnerAllocationConfigFromPools(runnerPools), runnerImage, policy, accepted,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner provisioning service: %w", err)
	}
	runnerMutations, err := runnercapability.NewMutationService(runnerRecords, idempotency, intentCoordinator)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner mutation service: %w", err)
	}
	runnerRemovals, err := runnercapability.NewRemovalService(runnerRecords, idempotency, intentCoordinator, accepted)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner removal service: %w", err)
	}
	diagnostics, err := executiondiagnostics.New(store, queue)
	if err != nil {
		return nil, err
	}
	lifecycle, maintenance, err := runnerassembly.NewLifecycle(logger, runnerRecords, runnerTokens, inputs, diagnostics)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Runner lifecycle: %w", err)
	}
	capabilities, err := runnerassembly.New(
		store,
		runnerRecords,
		idempotency,
		intentCoordinator,
		lifecycle,
		logger,
		accepted,
	)
	if err != nil {
		return nil, err
	}
	maintenance.ConfigureSoftwareUpdates(capabilities.Software)
	return &controllerRunnerComposition{
		software:         capabilities.Software,
		softwareExecutor: capabilities.SoftwareExecutor,
		pools:            runnerPools,
		lifecycle:        lifecycle,
		maintenance:      maintenance,
		provisioning:     runnerProvisioning,
		mutations:        runnerMutations,
		removals:         runnerRemovals,
		checks:           capabilities.Checks,
		checkExecutor:    capabilities.CheckExecutor,
	}, nil
}

func (composition *controllerRunnerComposition) AcceptedPreparations() []taskdependencies.Registration {
	registrations := composition.provisioning.AcceptedPreparations()
	registrations = append(registrations, composition.removals.AcceptedPreparations()...)
	registrations = append(registrations, composition.software.AcceptedPreparations()...)
	return append(registrations, composition.checks.AcceptedPreparations()...)
}
