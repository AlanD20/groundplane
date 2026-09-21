package runtimeconfiguration

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// runtimeConfigurationStore translates only the persistence mechanics used by
// the immutable file-source repository. It performs no source selection.
type Persistence interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type runtimeConfigurationStore struct{ store Persistence }

func New(store Persistence) (*Repository, error) {
	return newRepository(runtimeConfigurationStore{store: store})
}

func (adapter runtimeConfigurationStore) GetMany(
	ctx context.Context, keys []string, revision int64,
) (*GetManyResult, error) {
	read, err := adapter.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil || read == nil {
		return nil, err
	}
	result := &GetManyResult{
		ReadRevision: read.ReadRevision, Values: make([]*KeyValue, len(read.Values)),
	}
	for index, value := range read.Values {
		if value != nil {
			result.Values[index] = &KeyValue{
				Key: value.Key, Value: value.Value, ModRevision: value.ModRevision,
			}
		}
	}
	return result, nil
}

func (adapter runtimeConfigurationStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TxnResult, error) {
	compares := make([]etcdstore.Condition, len(conditions))
	for index, condition := range conditions {
		compares[index] = etcdstore.Condition{Key: condition.Key, ModRevision: condition.ModRevision}
	}
	writes := make([]etcdstore.Mutation, len(mutations))
	for index, mutation := range mutations {
		writes[index] = etcdstore.Mutation{
			Type:  etcdstore.MutationType(mutation.Type),
			Key:   mutation.Key,
			Value: mutation.Value,
		}
	}
	result, err := adapter.store.Transact(ctx, compares, writes)
	clearKeyValues(result.FailureReads)
	return TxnResult{
		Succeeded: result.Succeeded,
		Revision:  result.Revision,
	}, err
}

func clearKeyValues(values []*etcdstore.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}
