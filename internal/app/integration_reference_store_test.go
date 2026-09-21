package app

import (
	"context"
	"strings"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type connectorReferenceRaceStore struct {
	*memoryHierarchyStore
	referenceKey   string
	referenceValue []byte
	injected       bool
}

func (store *connectorReferenceRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if !store.injected && connectorReferenceConditionContains(conditions, store.referenceKey) {
		store.injected = true
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: store.referenceKey, Value: store.referenceValue,
		}}); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	for _, condition := range conditions {
		if !condition.Prefix {
			continue
		}
		result, err := store.memoryHierarchyStore.Range(ctx, testkeyvalue.RangeRequest{
			Prefix: condition.Key, Limit: 1,
		})
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		if len(result.Values) != 0 {
			failureReads, err := connectorReferenceFailureReads(ctx, store.memoryHierarchyStore, conditions)
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			return testkeyvalue.TransactionResult{
				Succeeded: false, Revision: result.ResponseRevision, FailureReads: failureReads,
			}, nil
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func connectorReferenceConditionContains(conditions []testkeyvalue.Condition, key string) bool {
	for _, condition := range conditions {
		if condition.Prefix && strings.HasPrefix(key, condition.Key) {
			return true
		}
	}
	return false
}

func connectorReferenceFailureReads(
	ctx context.Context,
	store *memoryHierarchyStore,
	conditions []testkeyvalue.Condition,
) ([]*testkeyvalue.KeyValue, error) {
	values := make([]*testkeyvalue.KeyValue, len(conditions))
	for index, condition := range conditions {
		if condition.Prefix {
			result, err := store.Range(ctx, testkeyvalue.RangeRequest{Prefix: condition.Key, Limit: 1})
			if err != nil {
				return nil, err
			}
			if len(result.Values) != 0 {
				value := result.Values[0]
				values[index] = &value
			}
			continue
		}
		result, err := store.Get(ctx, condition.Key)
		if err != nil {
			return nil, err
		}
		values[index] = result.Entry
	}
	return values, nil
}
