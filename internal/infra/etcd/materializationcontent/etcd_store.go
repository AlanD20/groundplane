package materializationcontent

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type materializationContentStore struct{ store etcdstore.Store }

func New(store etcdstore.Store) (*Repository, error) {
	return newRepository(materializationContentStore{store: store})
}

func (adapter materializationContentStore) GetMany(
	ctx context.Context,
	keys []string,
	revision int64,
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

func (adapter materializationContentStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	compares := make([]etcdstore.Condition, len(conditions))
	for index, condition := range conditions {
		compares[index] = etcdstore.Condition{Key: condition.Key, ModRevision: condition.ModRevision}
	}
	writes := make([]etcdstore.Mutation, len(mutations))
	for index, mutation := range mutations {
		writes[index] = etcdstore.Mutation{Type: etcdstore.MutationType(mutation.Type), Key: mutation.Key, Value: mutation.Value}
	}
	result, err := adapter.store.Transact(ctx, compares, writes)
	clearKeyValues(result.FailureReads)
	return TransactionResult{Succeeded: result.Succeeded, Revision: result.Revision}, err
}

func clearKeyValues(values []*etcdstore.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}
