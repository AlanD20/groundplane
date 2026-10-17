package workqueue

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Commit joins execution and operation-owned changes into one transaction. It
// never divides an oversized decision or retries a failed comparison using new
// state. A transport error may have committed; callers reconcile durable IDs
// before authorizing another attempt.
func (repository *Repository) Commit(ctx context.Context, changes ...Change) (keyvalue.TransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	joined, err := joinChanges(changes)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	budget, err := repository.store.MeasureTransaction(ctx, joined.Conditions, joined.Mutations)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if !budget.Fits() {
		return keyvalue.TransactionResult{}, invalid("execution decision exceeds the atomic storage limit")
	}
	result, err := repository.store.Transact(ctx, joined.Conditions, joined.Mutations)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if result.Revision <= 0 {
		return keyvalue.TransactionResult{}, errs.New(errs.KindInternal, "execution transaction returned no revision")
	}
	return result, nil
}

func joinChanges(changes []Change) (Change, error) {
	var joined Change
	conditions := make(map[keyvalue.Condition]struct{})
	revisions := make(map[string]int64)
	for _, change := range changes {
		for _, condition := range change.Conditions {
			if condition.Key == "" || condition.ModRevision < 0 || condition.Prefix && condition.ModRevision != 0 {
				return Change{}, invalid("execution transaction has an invalid comparison")
			}
			if !condition.Prefix {
				if revision, found := revisions[condition.Key]; found && revision != condition.ModRevision {
					return Change{}, invalid("execution transaction combines different resource revisions")
				}
				revisions[condition.Key] = condition.ModRevision
			}
			if _, found := conditions[condition]; !found {
				joined.Conditions = append(joined.Conditions, condition)
				conditions[condition] = struct{}{}
			}
		}
		for _, mutation := range change.Mutations {
			if mutation.Key == "" || mutation.Type != keyvalue.MutationPut && mutation.Type != keyvalue.MutationDelete ||
				mutation.Type == keyvalue.MutationPut && mutation.Prefix ||
				mutation.Type == keyvalue.MutationDelete && len(mutation.Value) != 0 {
				return Change{}, invalid("execution transaction has an invalid write")
			}
			for _, previous := range joined.Mutations {
				if previous.Key == mutation.Key || previous.Prefix && strings.HasPrefix(mutation.Key, previous.Key) ||
					mutation.Prefix && strings.HasPrefix(previous.Key, mutation.Key) {
					return Change{}, invalid("execution transaction writes the same resource more than once")
				}
			}
			joined.Mutations = append(joined.Mutations, mutation)
		}
	}
	if len(joined.Conditions) == 0 || len(joined.Mutations) == 0 {
		return Change{}, invalid("execution transaction requires comparisons and writes")
	}
	return joined, nil
}
