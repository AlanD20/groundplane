package environmentfence

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func AppendConditions(
	conditions []etcdstore.Condition,
	fence Evidence,
) ([]etcdstore.Condition, error) {
	indexes := make(map[string]int, len(conditions))
	for index, condition := range conditions {
		if condition.Key == "" || condition.Prefix {
			return nil, errs.New(errs.KindInternal, "environment mutation compare is invalid")
		}
		if _, duplicate := indexes[condition.Key]; duplicate {
			return nil, errs.New(errs.KindInternal, "environment mutation compare is duplicated")
		}
		indexes[condition.Key] = index
	}
	for _, required := range fence.TransactionConditions() {
		if index, found := indexes[required.Key]; found {
			if conditions[index].ModRevision != required.ModRevision {
				return nil, errs.New(
					errs.KindInternal,
					"environment mutation compare conflicts with its fence",
				)
			}
			continue
		}
		indexes[required.Key] = len(conditions)
		conditions = append(conditions, required)
	}
	return conditions, nil
}

func ValidateTransactionBudget(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) error {
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return errs.New(
			errs.KindValidationFailed,
			"environment mutation exceeds the atomic transaction limit",
		)
	}
	return nil
}
