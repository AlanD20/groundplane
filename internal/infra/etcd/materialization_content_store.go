package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/materializationcontent"
)

type materializationContentStore struct{ store hierarchyStore }

func NewMaterializationContentRepository(store hierarchyStore) (*materializationcontent.Repository, error) {
	return materializationcontent.NewRepository(materializationContentStore{store: store})
}

func (adapter materializationContentStore) GetMany(
	ctx context.Context,
	keys []string,
	revision int64,
) (*materializationcontent.GetManyResult, error) {
	read, err := adapter.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil || read == nil {
		return nil, err
	}
	result := &materializationcontent.GetManyResult{
		ReadRevision: read.ReadRevision, Values: make([]*materializationcontent.KeyValue, len(read.Values)),
	}
	for index, value := range read.Values {
		if value != nil {
			result.Values[index] = &materializationcontent.KeyValue{
				Key: value.Key, Value: value.Value, ModRevision: value.ModRevision,
			}
		}
	}
	return result, nil
}

func (adapter materializationContentStore) Transact(
	ctx context.Context,
	conditions []materializationcontent.Condition,
	mutations []materializationcontent.Mutation,
) (materializationcontent.TransactionResult, error) {
	compares := make([]Condition, len(conditions))
	for index, condition := range conditions {
		compares[index] = Condition{Key: condition.Key, ModRevision: condition.ModRevision}
	}
	writes := make([]Mutation, len(mutations))
	for index, mutation := range mutations {
		writes[index] = Mutation{Type: MutationType(mutation.Type), Key: mutation.Key, Value: mutation.Value}
	}
	result, err := adapter.store.Transact(ctx, compares, writes)
	clearKeyValues(result.FailureReads)
	return materializationcontent.TransactionResult{Succeeded: result.Succeeded, Revision: result.Revision}, err
}
