package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) EnsureRunnerNetworkPool(
	ctx context.Context,
	config runnerallocation.RunnerAllocationConfig,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if _, err := config.Validate(); err != nil {
		return err
	}
	result, err := repository.store.Get(ctx, systemPoolRegistryKey)
	if err != nil {
		return err
	}
	if result == nil {
		return errs.New(errs.KindInternal, "system pool registry read is missing")
	}
	if result.Entry != nil {
		return validateReservedRunnerNetworkPool(config, result.Entry.Value)
	}
	registry := runnerallocation.SystemPoolRegistry{
		RunnerNetworkPool: config.RunnerPool.String(), Reservations: map[string]string{},
	}
	value, err := recordcodec.Encode("system_pool_registry", registry)
	if err != nil {
		return err
	}
	defer clear(value)
	transaction, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{{Key: systemPoolRegistryKey}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: systemPoolRegistryKey, Value: value}},
	)
	if err != nil {
		return err
	}
	if transaction.Succeeded {
		return nil
	}
	if len(transaction.FailureReads) != 1 || transaction.FailureReads[0] == nil {
		return errs.New(errs.KindInternal, "system pool bootstrap compare evidence is incomplete")
	}
	return validateReservedRunnerNetworkPool(config, transaction.FailureReads[0].Value)
}

func validateReservedRunnerNetworkPool(config runnerallocation.RunnerAllocationConfig, value []byte) error {
	if len(value) > runnerrecord.MaximumRunnerPersistenceBytes {
		return corruptSystemPoolRegistry()
	}
	registry, err := decodeSystemPoolRegistry(value)
	if err != nil || registry.Validate(config.SystemPool) != nil {
		return corruptSystemPoolRegistry()
	}
	if registry.RunnerNetworkPool != config.RunnerPool.String() {
		return errs.New(
			errs.KindStateConflict,
			"configured runner network pool does not match its bootstrap reservation",
		)
	}
	return nil
}
