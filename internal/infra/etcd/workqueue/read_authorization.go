package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// AuthorizeRead proves all captured ownership comparisons at one current,
// linearizable revision before returning captured data. It grants no new effect,
// writes no execution history and cannot replace Commit for a state transition.
func (repository *Repository) AuthorizeRead(ctx context.Context, conditions []keyvalue.Condition) (bool, error) {
	if len(conditions) == 0 || len(conditions) > keyvalue.MaximumOperations {
		return false, invalid("execution read authorization requires bounded ownership comparisons")
	}
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		if condition.Key == "" || condition.ModRevision < 0 || condition.Prefix {
			return false, invalid("execution read authorization has an invalid comparison")
		}
		keys[index] = condition.Key
	}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearValues(read.Values)
	for index, condition := range conditions {
		if !keyvalue.ConditionMatchesRead(condition, read.Values[index]) {
			return false, nil
		}
	}
	return true, nil
}
