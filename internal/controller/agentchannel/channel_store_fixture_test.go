package agentchannel

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"io"
	"sort"
	"strings"
	"sync"
)

type channelMemoryStore struct {
	mu       sync.Mutex
	revision int64
	values   map[string]testkeyvalue.KeyValue
}

func newChannelMemoryStore() *channelMemoryStore {
	return &channelMemoryStore{revision: 1, values: make(map[string]testkeyvalue.KeyValue)}
}

func (store *channelMemoryStore) Health(context.Context) error { return nil }

func (store *channelMemoryStore) Get(ctx context.Context, key string) (*testkeyvalue.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return &testkeyvalue.GetResult{Entry: store.valueLocked(key), ReadRevision: store.revision}, nil
}

func (store *channelMemoryStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueLocked(key)
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &testkeyvalue.GetManyResult{
		Values: values, ReadRevision: readRevision, ResponseRevision: store.revision,
	}, nil
}

func (store *channelMemoryStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	return result.Revision, err
}

func (store *channelMemoryStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}})
	return result.Revision, err
}

func (store *channelMemoryStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0)
	for key := range store.values {
		if !strings.HasPrefix(key, request.Prefix) {
			continue
		}
		if request.StartExclusive != "" &&
			((!request.Descending && key <= request.StartExclusive) ||
				(request.Descending && key >= request.StartExclusive)) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if request.Descending {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	more := int64(len(keys)) > request.Limit
	if more {
		keys = keys[:request.Limit]
	}
	values := make([]testkeyvalue.KeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, *store.valueLocked(key))
	}
	readRevision := request.Revision
	if readRevision == 0 {
		readRevision = store.revision
	}
	return &testkeyvalue.RangeResult{
		Values: values, ReadRevision: readRevision, ResponseRevision: store.revision, More: more,
	}, nil
}

func (store *channelMemoryStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *channelMemoryStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, condition := range conditions {
		value := store.conditionValueLocked(condition)
		actual := int64(0)
		if value != nil {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
			for index, candidate := range conditions {
				failureReads[index] = store.conditionValueLocked(candidate)
			}
			return testkeyvalue.TransactionResult{
				Revision: store.revision, FailureReads: failureReads,
			}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		switch mutation.Type {
		case testkeyvalue.MutationPut:
			current := store.values[mutation.Key]
			store.values[mutation.Key] = testkeyvalue.KeyValue{
				Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
				Version: current.Version + 1, ModRevision: store.revision,
			}
		case testkeyvalue.MutationDelete:
			if mutation.Prefix {
				for key := range store.values {
					if strings.HasPrefix(key, mutation.Key) {
						delete(store.values, key)
					}
				}
			} else {
				delete(store.values, mutation.Key)
			}
		default:
			return testkeyvalue.TransactionResult{}, errors.New("unexpected transaction mutation")
		}
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *channelMemoryStore) Watch(
	context.Context, string,

	int64,
) (*testkeyvalue.WatchStream, error) {
	return nil, errors.New("unexpected Watch")
}

func (store *channelMemoryStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("unexpected Snapshot")
}

func (store *channelMemoryStore) Close() error { return nil }

func (store *channelMemoryStore) valueLocked(key string) *testkeyvalue.KeyValue {
	value, ok := store.values[key]
	if !ok {
		return nil
	}
	value.Value = append([]byte(nil), value.Value...)
	return &value
}

func (store *channelMemoryStore) conditionValueLocked(condition testkeyvalue.Condition) *testkeyvalue.KeyValue {
	if !condition.Prefix {
		return store.valueLocked(condition.Key)
	}
	keys := make([]string, 0)
	for key := range store.values {
		if strings.HasPrefix(key, condition.Key) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return store.valueLocked(keys[0])
}
