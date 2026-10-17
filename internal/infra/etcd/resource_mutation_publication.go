package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func resourcePublicationConditions(groups ...[]keyvalue.Condition) ([]keyvalue.Condition, error) {
	var result []keyvalue.Condition
	seen := make(map[string]keyvalue.Condition)
	for _, group := range groups {
		for _, condition := range group {
			if previous, exists := seen[condition.Key]; exists {
				if previous != condition {
					return nil, errs.New(errs.KindStateConflict, "resource publication source revisions disagree")
				}
				continue
			}
			seen[condition.Key] = condition
			result = append(result, condition)
		}
	}
	return result, nil
}

func resourceWorkPublicationConflict(conditions []keyvalue.Condition) idempotencyPlanClassifier {
	return func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "resource publication comparison evidence is incomplete")
		}
		for index, condition := range conditions {
			if !keyvalue.ConditionMatchesRead(condition, values[index]) {
				return errs.New(errs.KindStateConflict, "resource desired state or captured publication ownership changed")
			}
		}
		return nil
	}
}
