package environmentfence

import (
	"context"
	"strings"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A point read cannot prove a prefix empty. Prepare the same bounded range
// evidence that etcd returns when its atomic prefix-absence comparison fails.
func readMutationComparisonEvidence(ctx context.Context, store mutationBindingStore,
	conditions []etcdstore.Condition, revision int64,
) (*etcdstore.GetManyResult, error) {
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	prepared, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if prepared == nil || prepared.ReadRevision != revision || len(prepared.Values) != len(conditions) {
		if prepared != nil {
			etcdstore.ClearValues(prepared.Values)
		}
		return nil, errs.New(errs.KindInternal, "environment mutation domain read is invalid")
	}
	for index, condition := range conditions {
		if !condition.Prefix {
			continue
		}
		if condition.ModRevision != 0 {
			etcdstore.ClearValues(prepared.Values)
			return nil, errs.New(errs.KindInternal, "environment mutation prefix compare must require absence")
		}
		page, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: condition.Key, Limit: 1, Revision: revision})
		if err != nil {
			etcdstore.ClearValues(prepared.Values)
			return nil, err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 1 ||
			page.More && len(page.Values) == 0 ||
			len(page.Values) == 1 &&
				(!strings.HasPrefix(page.Values[0].Key, condition.Key) || page.Values[0].ModRevision <= 0) {
			if page != nil {
				etcdstore.ClearRangeValues(page.Values)
			}
			etcdstore.ClearValues(prepared.Values)
			return nil, errs.New(errs.KindInternal, "environment mutation prefix read is invalid")
		}
		if prepared.Values[index] != nil {
			clear(prepared.Values[index].Value)
		}
		prepared.Values[index] = nil
		if len(page.Values) == 1 {
			prepared.Values[index] = &page.Values[0]
		}
	}
	return prepared, nil
}

func mutationComparisonMatchesRead(condition etcdstore.Condition, value *etcdstore.KeyValue) bool {
	if condition.Prefix {
		return condition.ModRevision == 0 && value == nil
	}
	return etcdstore.ConditionMatchesRead(condition, value)
}
