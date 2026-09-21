package backingservices

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type backingCreationStoreVersion struct {
	revision int64
	present  bool
	value    testkeyvalue.KeyValue
}

// backingCreationStore is the smallest revision-aware Store needed to compose
// the real repositories exercised by the custom backing creation test.
type backingCreationStore struct {
	mu       sync.Mutex
	values   map[string]testkeyvalue.KeyValue
	history  map[string][]backingCreationStoreVersion
	revision int64
}

func newBackingCreationStore() *backingCreationStore {
	return &backingCreationStore{
		values:   make(map[string]testkeyvalue.KeyValue),
		history:  make(map[string][]backingCreationStoreVersion),
		revision: 1,
	}
}

func (*backingCreationStore) Health(context.Context) error { return nil }
func (*backingCreationStore) Close() error                 { return nil }

func (store *backingCreationStore) Get(
	_ context.Context,
	key string,
) (*testkeyvalue.GetResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := &testkeyvalue.GetResult{ReadRevision: store.revision}
	result.Entry = store.valueAt(key, store.revision)
	return result, nil
}

func (store *backingCreationStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	view := request.Revision
	if view == 0 {
		view = store.revision
	}
	result := &testkeyvalue.GetManyResult{
		Values:       make([]*testkeyvalue.KeyValue, len(request.Keys)),
		ReadRevision: view, ResponseRevision: store.revision,
	}
	for index, key := range request.Keys {
		result.Values[index] = store.valueAt(key, view)
	}
	return result, nil
}

func (store *backingCreationStore) Range(
	_ context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	view := request.Revision
	if view == 0 {
		view = store.revision
	}
	keys := make([]string, 0, len(store.history))
	for key := range store.history {
		if !strings.HasPrefix(key, request.Prefix) || store.valueAt(key, view) == nil {
			continue
		}
		if request.StartExclusive != "" && ((!request.Descending && key <= request.StartExclusive) ||
			(request.Descending && key >= request.StartExclusive)) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if request.Descending {
		for left, right := 0, len(keys)-1; left < right; left, right = left+1, right-1 {
			keys[left], keys[right] = keys[right], keys[left]
		}
	}
	result := &testkeyvalue.RangeResult{
		ReadRevision: view, ResponseRevision: store.revision,
		More: request.Limit > 0 && len(keys) > int(request.Limit),
	}
	if result.More {
		keys = keys[:request.Limit]
	}
	result.Values = make([]testkeyvalue.KeyValue, 0, len(keys))
	for _, key := range keys {
		result.Values = append(result.Values, *store.valueAt(key, view))
	}
	return result, nil
}

func (store *backingCreationStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: key, Value: value,
	}})
	return result.Revision, err
}

func (store *backingCreationStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationDelete, Key: key,
	}})
	return result.Revision, err
}

func (*backingCreationStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *backingCreationStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if failed := store.failedConditions(conditions); failed != nil {
		return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: failed}, nil
	}
	store.revision++
	for _, mutation := range mutations {
		store.apply(mutation)
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *backingCreationStore) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	return store.Transact(ctx, conditions, mutations)
}

func (*backingCreationStore) ValidateBlueprintTaskTerminal(
	_ context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) error {
	return envelope.ValidateBudget("")
}

func (store *backingCreationStore) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	return store.Transact(ctx, conditions, mutations)
}

func (*backingCreationStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, errors.New("unexpected watch")
}

func (*backingCreationStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("unexpected snapshot")
}

func (store *backingCreationStore) failedConditions(
	conditions []testkeyvalue.Condition,
) []*testkeyvalue.KeyValue {
	matched := true
	for _, condition := range conditions {
		if condition.Prefix {
			for key := range store.values {
				if strings.HasPrefix(key, condition.Key) {
					matched = false
					break
				}
			}
			continue
		}
		actual := int64(0)
		if value, found := store.values[condition.Key]; found {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			matched = false
		}
	}
	if matched {
		return nil
	}
	reads := make([]*testkeyvalue.KeyValue, len(conditions))
	for index, condition := range conditions {
		if condition.Prefix {
			keys := make([]string, 0)
			for key := range store.values {
				if strings.HasPrefix(key, condition.Key) {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			if len(keys) > 0 {
				value := cloneBackingCreationValue(store.values[keys[0]])
				reads[index] = &value
			}
			continue
		}
		if value, found := store.values[condition.Key]; found {
			cloned := cloneBackingCreationValue(value)
			reads[index] = &cloned
		}
	}
	return reads
}

func (store *backingCreationStore) apply(mutation testkeyvalue.Mutation) {
	if mutation.Type == testkeyvalue.MutationPut {
		version := int64(1)
		if current, found := store.values[mutation.Key]; found {
			version = current.Version + 1
		}
		value := testkeyvalue.KeyValue{
			Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
			Version: version, ModRevision: store.revision,
		}
		store.values[mutation.Key] = value
		store.history[mutation.Key] = append(store.history[mutation.Key], backingCreationStoreVersion{
			revision: store.revision, present: true, value: cloneBackingCreationValue(value),
		})
		return
	}
	keys := []string{mutation.Key}
	if mutation.Prefix {
		keys = keys[:0]
		for key := range store.values {
			if strings.HasPrefix(key, mutation.Key) {
				keys = append(keys, key)
			}
		}
	}
	for _, key := range keys {
		if _, found := store.values[key]; !found {
			continue
		}
		delete(store.values, key)
		store.history[key] = append(store.history[key], backingCreationStoreVersion{revision: store.revision})
	}
}

func (store *backingCreationStore) valueAt(key string, revision int64) *testkeyvalue.KeyValue {
	versions := store.history[key]
	for index := len(versions) - 1; index >= 0; index-- {
		if versions[index].revision <= revision {
			if !versions[index].present {
				return nil
			}
			value := cloneBackingCreationValue(versions[index].value)
			return &value
		}
	}
	return nil
}

func cloneBackingCreationValue(value testkeyvalue.KeyValue) testkeyvalue.KeyValue {
	value.Value = append([]byte(nil), value.Value...)
	return value
}
