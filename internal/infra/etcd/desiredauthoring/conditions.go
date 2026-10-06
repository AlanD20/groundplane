package desiredauthoring

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func CheckConditions(ctx context.Context, store Store, conditions []keyvalue.Condition) (*keyvalue.GetManyResult, bool, error) {
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	state, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return nil, false, err
	}
	if state == nil || len(state.Values) != len(keys) {
		return nil, false, errs.New(errs.KindInternal, "desired mutation preparation evidence is incomplete")
	}
	for index, condition := range conditions {
		if !keyvalue.ConditionMatchesRead(condition, state.Values[index]) {
			return state, false, nil
		}
	}
	return state, true, nil
}

func ClassifyRouteConditions(conditions []keyvalue.Condition) Classifier {
	return func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Route desired head compare evidence is incomplete")
		}
		for index, condition := range conditions {
			if !keyvalue.ConditionMatchesRead(condition, values[index]) {
				return errs.New(errs.KindStateConflict, "Route desired head changed")
			}
		}
		return errs.New(errs.KindStateConflict, "Route desired head changed")
	}
}
