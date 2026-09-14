package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/runtimeconfiguration"
)

// runtimeConfigurationStore translates only the persistence mechanics used by
// the immutable file-source repository. It performs no source selection.
type runtimeConfigurationStore struct{ store hierarchyStore }

func NewRuntimeConfigurationRepository(store hierarchyStore) (*runtimeconfiguration.Repository, error) {
	return runtimeconfiguration.NewRepository(runtimeConfigurationStore{store: store})
}

func (adapter runtimeConfigurationStore) GetMany(
	ctx context.Context, keys []string, revision int64,
) (*runtimeconfiguration.GetManyResult, error) {
	read, err := adapter.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil || read == nil {
		return nil, err
	}
	result := &runtimeconfiguration.GetManyResult{
		ReadRevision: read.ReadRevision, Values: make([]*runtimeconfiguration.KeyValue, len(read.Values)),
	}
	for index, value := range read.Values {
		if value != nil {
			result.Values[index] = &runtimeconfiguration.KeyValue{
				Key: value.Key, Value: value.Value, ModRevision: value.ModRevision,
			}
		}
	}
	return result, nil
}

func (adapter runtimeConfigurationStore) Transact(
	ctx context.Context,
	conditions []runtimeconfiguration.Condition,
	mutations []runtimeconfiguration.Mutation,
) (runtimeconfiguration.TxnResult, error) {
	compares := make([]Condition, len(conditions))
	for index, condition := range conditions {
		compares[index] = Condition{Key: condition.Key, ModRevision: condition.ModRevision}
	}
	writes := make([]Mutation, len(mutations))
	for index, mutation := range mutations {
		writes[index] = Mutation{
			Type:  MutationType(mutation.Type),
			Key:   mutation.Key,
			Value: mutation.Value,
		}
	}
	result, err := adapter.store.Transact(ctx, compares, writes)
	clearKeyValues(result.FailureReads)
	return runtimeconfiguration.TxnResult{
		Succeeded: result.Succeeded,
		Revision:  result.Revision,
	}, err
}
