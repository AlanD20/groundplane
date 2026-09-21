package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type zoneRemovalTransactionPhase string

const (
	zoneRemovalTransactionBegin                    zoneRemovalTransactionPhase = "begin"
	zoneRemovalTransactionRetry                    zoneRemovalTransactionPhase = "retry"
	zoneRemovalTransactionCompletedAcknowledgement zoneRemovalTransactionPhase = "completed acknowledgement"
	zoneRemovalTransactionFailedAcknowledgement    zoneRemovalTransactionPhase = "failed acknowledgement"
)

func zoneRemovalTransactionBudgetValidator(
	phase zoneRemovalTransactionPhase,
) func([]etcdstore.Condition, []etcdstore.Mutation) error {
	return func(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) error {
		return validateZoneRemovalTransactionBudget(phase, conditions, mutations)
	}
}

func validateZoneRemovalTransactionBudget(
	phase zoneRemovalTransactionPhase,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) error {
	switch phase {
	case zoneRemovalTransactionBegin,
		zoneRemovalTransactionRetry,
		zoneRemovalTransactionCompletedAcknowledgement,
		zoneRemovalTransactionFailedAcknowledgement:
	default:
		return errs.New(errs.KindInternal, "Zone removal transaction phase is invalid")
	}
	if len(conditions)+len(mutations) >= etcdstore.MaximumOperations {
		return errs.Newf(
			errs.KindValidationFailed,
			"Zone removal %s transaction must remain below %d operations",
			phase,
			etcdstore.MaximumOperations,
		)
	}
	return nil
}

func (repository *TaskRepository) transactZoneRemovalTaskLifecycle(
	ctx context.Context,
	task TaskRecord,
	phase zoneRemovalTransactionPhase,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	if task.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		if err := validateZoneRemovalTransactionBudget(phase, conditions, mutations); err != nil {
			return etcdstore.TransactionResult{}, err
		}
	}
	return repository.store.Transact(ctx, conditions, mutations)
}
