package buildcredentials

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func mergeConditions(left, right []keyvalue.Condition) ([]keyvalue.Condition, error) {
	group := make([]keyvalue.Condition, 0, len(left)+len(right))
	seen := make(map[string]int64, cap(group))
	for _, conditions := range [][]keyvalue.Condition{left, right} {
		for _, condition := range conditions {
			if condition.Key == "" || condition.Prefix || condition.ModRevision < 0 {
				return nil, denied()
			}
			if previous, exists := seen[condition.Key]; exists {
				if previous != condition.ModRevision {
					return nil, errs.New(
						errs.KindStateConflict,
						"build job authorization changed while it was being verified",
					)
				}
				continue
			}
			seen[condition.Key] = condition.ModRevision
			group = append(group, condition)
		}
	}
	return group, nil
}

func (service *Service) checkConditions(ctx context.Context, conditions []keyvalue.Condition) error {
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	result, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return denied()
	}
	defer keyvalue.ClearValues(result.Values)
	for index, value := range result.Values {
		expected := conditions[index]
		if value == nil {
			if expected.ModRevision != 0 {
				return denied()
			}
			continue
		}
		if value.Key != expected.Key || value.ModRevision <= 0 || value.ModRevision != expected.ModRevision {
			return denied()
		}
	}
	return nil
}
