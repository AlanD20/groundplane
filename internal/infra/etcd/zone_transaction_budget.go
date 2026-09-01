package etcd

import (
	"context"

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
) func([]Condition, []Mutation) error {
	return func(conditions []Condition, mutations []Mutation) error {
		return validateZoneRemovalTransactionBudget(phase, conditions, mutations)
	}
}

func validateZoneRemovalTransactionBudget(
	phase zoneRemovalTransactionPhase,
	conditions []Condition,
	mutations []Mutation,
) error {
	switch phase {
	case zoneRemovalTransactionBegin,
		zoneRemovalTransactionRetry,
		zoneRemovalTransactionCompletedAcknowledgement,
		zoneRemovalTransactionFailedAcknowledgement:
	default:
		return errs.New(errs.KindInternal, "Zone removal transaction phase is invalid")
	}
	if len(conditions)+len(mutations) >= maximumTransactionOperations {
		return errs.Newf(
			errs.KindValidationFailed,
			"Zone removal %s transaction must remain below %d operations",
			phase,
			maximumTransactionOperations,
		)
	}
	return nil
}

func (repository *TaskRepository) transactZoneRemovalTaskLifecycle(
	ctx context.Context,
	task TaskRecord,
	phase zoneRemovalTransactionPhase,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if task.Params[TaskZoneRemovalOperationParam] != "" {
		if err := validateZoneRemovalTransactionBudget(phase, conditions, mutations); err != nil {
			return TransactionResult{}, err
		}
	}
	return repository.store.Transact(ctx, conditions, mutations)
}
