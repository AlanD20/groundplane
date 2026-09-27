package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"net/netip"
)

type controllerRunnerComposition struct {
	pools        config.AllocationPools
	policy       corerunner.IsolationPolicy
	tokens       *runnercapability.TokenBroker
	provisioning *runnercapability.ProvisioningService
	mutations    *runnercapability.MutationService
	removals     *runnercapability.RemovalService
}

func newControllerRunnerComposition(
	cfg config.ControllerConfig,
	runnerRecords *etcd.RunnerRepository,
	tasks *etcd.TaskRepository,
	hierarchyRecords *etcd.HierarchyRepository,
	idempotency *etcd.IdempotencyRepository,
	intentCoordinator *requestidempotency.Coordinator,
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
	runnerProvisioning, err := runnercapability.NewProvisioningService(
		runnerRecords, tasks, hierarchyRecords, idempotency, intentCoordinator, runnerTokens,
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
	return &controllerRunnerComposition{
		pools: runnerPools, policy: policy, tokens: runnerTokens, provisioning: runnerProvisioning,
		mutations: runnerMutations, removals: runnerRemovals,
	}, nil
}
