package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (s *store) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
		return etcdstore.TransactionResult{}, err
	}
	return s.transact(ctx, conditions, mutations)
}

func validateEnvironmentBlueprintTransactionBudget(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) error {
	if len(conditions) <= maximumEnvironmentBlueprintTransactionOperationsPerArm &&
		len(mutations) <= maximumEnvironmentBlueprintTransactionOperationsPerArm {
		return nil
	}
	return errs.Newf(
		errs.KindValidationFailed,
		"Environment Blueprint publication exceeds a %d-operation transaction arm (%d/%d/%d)",
		maximumEnvironmentBlueprintTransactionOperationsPerArm,
		len(conditions), len(mutations), len(conditions),
	)
}

type environmentBlueprintTransactionStore interface {
	TransactEnvironmentBlueprint(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

func executeEnvironmentBlueprintTransaction(
	ctx context.Context,
	store environmentBlueprintTransactionStore,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	if store == nil {
		return etcdstore.TransactionResult{}, errs.New(errs.KindInternal, "Environment Blueprint transaction store is required")
	}
	if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
		return etcdstore.TransactionResult{}, err
	}
	if len(mutations) == 0 {
		return etcdstore.TransactionResult{}, errs.New(errs.KindValidationFailed, "etcd transaction requires a mutation")
	}
	physicalConditions := make([]string, len(conditions))
	for index := range conditions {
		physicalConditions[index] = conditions[index].Key
	}
	physicalMutations := make([]string, len(mutations))
	for index := range mutations {
		physicalMutations[index] = mutations[index].Key
	}
	if transactionRequest(
		conditions,
		mutations,
		physicalConditions,
		physicalMutations,
	).Size() >
		etcdstore.MaximumBytes {
		return etcdstore.TransactionResult{}, errs.New(
			errs.KindValidationFailed, "etcd transaction exceeds the 1 MiB serialized request limit",
		)
	}
	return store.TransactEnvironmentBlueprint(ctx, conditions, mutations)
}
