package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func bootstrapRunnerNetworkPool(
	ctx context.Context,
	store etcd.Store,
	controllerConfig config.ControllerConfig,
) error {
	pools, err := controllerConfig.AllocationPools()
	if err != nil {
		return err
	}
	repository, err := etcd.NewRunnerRepository(store)
	if err != nil {
		return err
	}
	return repository.EnsureRunnerNetworkPool(ctx, etcd.RunnerAllocationConfigFromPools(pools))
}
