package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type controllerRunnerComposition struct {
	pools        config.AllocationPools
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
	runnerProvisioning, err := runnercapability.NewProvisioningService(
		runnerRecords, tasks, hierarchyRecords, idempotency, intentCoordinator, runnerTokens,
		runnerallocation.RunnerAllocationConfigFromPools(runnerPools), cfg.Runner.Image,
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
		pools: runnerPools, tokens: runnerTokens, provisioning: runnerProvisioning,
		mutations: runnerMutations, removals: runnerRemovals,
	}, nil
}
