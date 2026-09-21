package app

import (
	"context"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type memoryVersion struct {
	revision int64
	value    []byte
	present  bool
}

type memoryHierarchyStore struct {
	revision int64
	history  map[string][]memoryVersion
}

func newMemoryHierarchyStore() *memoryHierarchyStore {
	return &memoryHierarchyStore{history: make(map[string][]memoryVersion)}
}

func (store *memoryHierarchyStore) Get(_ context.Context, key string) (*testkeyvalue.GetResult, error) {
	value := store.valueAt(key, store.revision)
	return &testkeyvalue.GetResult{Entry: value, ReadRevision: store.revision}, nil
}

func (store *memoryHierarchyStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueAt(key, revision)
	}
	return &testkeyvalue.GetManyResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision,
	}, nil
}

func (store *memoryHierarchyStore) Range(
	_ context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	keys := make([]string, 0)
	for key := range store.history {
		if !strings.HasPrefix(key, request.Prefix) || key <= request.StartExclusive {
			continue
		}
		if store.valueAt(key, revision) != nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	more := int64(len(keys)) > request.Limit
	if more {
		keys = keys[:request.Limit]
	}
	values := make([]testkeyvalue.KeyValue, 0, len(keys))
	for _, key := range keys {
		value := store.valueAt(key, revision)
		values = append(values, *value)
	}
	return &testkeyvalue.RangeResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision, More: more,
	}, nil
}

func (store *memoryHierarchyStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *memoryHierarchyStore) Transact(
	_ context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, condition := range conditions {
		value := store.valueAt(condition.Key, store.revision)
		actualRevision := int64(0)
		if value != nil {
			actualRevision = value.ModRevision
		}
		if actualRevision != condition.ModRevision {
			failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
			for index, failedCondition := range conditions {
				failureReads[index] = store.valueAt(failedCondition.Key, store.revision)
			}
			return testkeyvalue.TransactionResult{
				Succeeded: false, Revision: store.revision, FailureReads: failureReads,
			}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		if mutation.Type == testkeyvalue.MutationDelete && mutation.Prefix {
			for key := range store.history {
				if strings.HasPrefix(key, mutation.Key) {
					store.history[key] = append(
						store.history[key],
						memoryVersion{revision: store.revision},
					)
				}
			}
			continue
		}
		version := memoryVersion{revision: store.revision}
		switch mutation.Type {
		case testkeyvalue.MutationPut:
			version.present = true
			version.value = append([]byte(nil), mutation.Value...)
		case testkeyvalue.MutationDelete:
		default:
			return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "fake store received invalid mutation")
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], version)
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryHierarchyStore) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	return store.Transact(ctx, conditions, mutations)
}

func (store *memoryHierarchyStore) valueAt(key string, revision int64) *testkeyvalue.KeyValue {
	versions := store.history[key]
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		if version.revision > revision {
			continue
		}
		if !version.present {
			return nil
		}
		keyVersion := int64(0)
		for previous := index; previous >= 0 && versions[previous].present; previous-- {
			keyVersion++
		}
		return &testkeyvalue.KeyValue{
			Key: key, Value: append([]byte(nil), version.value...),
			Version: keyVersion, ModRevision: version.revision,
		}
	}
	return nil
}
