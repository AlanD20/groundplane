package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) transact(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		return etcdstore.TransactionResult{}, err
	}
	return repository.store.Transact(ctx, conditions, mutations)
}

func validateBackupRuntimeTransactionBounds(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) error {
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return errs.Newf(
			errs.KindInternal,
			"backup runtime transaction exceeds the %d-operation limit",
			etcdstore.MaximumOperations,
		)
	}
	size := 0
	for _, condition := range conditions {
		size += len(condition.Key) + 64
	}
	for _, mutation := range mutations {
		size += len(mutation.Key) + len(mutation.Value) + 64
	}
	if size > maximumBackupRuntimeTransactionBytes {
		return errs.New(
			errs.KindInternal,
			"backup runtime transaction exceeds the 768 KiB preflight limit",
		)
	}
	return nil
}

func clearBackupRuntimeMutations(mutations []etcdstore.Mutation) {
	for index := range mutations {
		clear(mutations[index].Value)
		mutations[index].Value = nil
	}
}
