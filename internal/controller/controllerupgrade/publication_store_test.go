package controllerupgrade

import (
	"context"
	"io"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// This storage-boundary double exercises the real Task and protected-marker
// repositories. Tests are sequential; unsupported unrelated capabilities fail.
type publicationStore struct {
	entries           map[string]testkeyvalue.KeyValue
	history           map[int64]map[string]testkeyvalue.KeyValue
	revision          int64
	transactions      int
	loseResponse      bool
	loseResponseMatch func([]testkeyvalue.Mutation) bool
	readError         error
	beforeCommit      func()
}

func (store *publicationStore) entry(key string) *testkeyvalue.KeyValue {
	return publicationEntry(store.entries, key)
}

func publicationEntry(entries map[string]testkeyvalue.KeyValue, key string) *testkeyvalue.KeyValue {
	value, found := entries[key]
	if !found {
		return nil
	}
	value.Value = append([]byte(nil), value.Value...)
	return &value
}
func (store *publicationStore) Get(ctx context.Context, key string) (*testkeyvalue.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &testkeyvalue.GetResult{Entry: store.entry(key), ReadRevision: store.revision}, nil
}

func (store *publicationStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store.readError != nil {
		return nil, store.readError
	}
	entries, revision, err := store.at(request.Revision)
	if err != nil {
		return nil, err
	}
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = publicationEntry(entries, key)
	}
	return &testkeyvalue.GetManyResult{
		Values:           values,
		ReadRevision:     revision,
		ResponseRevision: store.revision,
	}, nil
}

func (store *publicationStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	if store.readError != nil {
		return nil, store.readError
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, revision, err := store.at(request.Revision)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0)
	for key := range entries {
		if strings.HasPrefix(key, request.Prefix) && key > request.StartExclusive {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if request.Descending {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	more := request.Limit > 0 && int64(len(keys)) > request.Limit
	if more {
		keys = keys[:request.Limit]
	}
	values := make([]testkeyvalue.KeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, *publicationEntry(entries, key))
	}
	return &testkeyvalue.RangeResult{
		Values:           values,
		ReadRevision:     revision,
		ResponseRevision: store.revision,
		More:             more,
	}, nil
}

func (store *publicationStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *publicationStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.remember()
	if store.beforeCommit != nil {
		hook := store.beforeCommit
		store.beforeCommit = nil
		hook()
	}
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	for _, condition := range conditions {
		matches := store.entries[condition.Key].ModRevision == condition.ModRevision
		if condition.Prefix {
			if condition.ModRevision != 0 {
				return testkeyvalue.TransactionResult{}, unexpectedStorage()
			}
			matches = true
			for key := range store.entries {
				if strings.HasPrefix(key, condition.Key) {
					matches = false
					break
				}
			}
		}
		if !matches {
			reads := make([]*testkeyvalue.KeyValue, len(conditions))
			for index, item := range conditions {
				reads[index] = store.entry(item.Key)
			}
			return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: reads}, nil
		}
	}
	store.revision++
	store.transactions++
	for _, mutation := range mutations {
		if mutation.Prefix {
			if mutation.Type != testkeyvalue.MutationDelete {
				return testkeyvalue.TransactionResult{}, unexpectedStorage()
			}
			for key := range store.entries {
				if strings.HasPrefix(key, mutation.Key) {
					delete(store.entries, key)
				}
			}
			continue
		}
		if mutation.Type == testkeyvalue.MutationDelete {
			delete(store.entries, mutation.Key)
			continue
		}
		old := store.entries[mutation.Key]
		store.entries[mutation.Key] = testkeyvalue.KeyValue{
			Key:         mutation.Key,
			Value:       append([]byte(nil), mutation.Value...),
			Version:     old.Version + 1,
			ModRevision: store.revision,
		}
	}
	store.remember()
	if store.loseResponse && (store.loseResponseMatch == nil || store.loseResponseMatch(mutations)) {
		store.loseResponse = false
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindStorageUnavailable, "lost committed response")
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}
func (*publicationStore) Health(context.Context) error { return unexpectedStorage() }
func (*publicationStore) Put(context.Context, string, []byte) (int64, error) {
	return 0, unexpectedStorage()
}
func (*publicationStore) Delete(context.Context, string) (int64, error) {
	return 0, unexpectedStorage()
}
func (*publicationStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, unexpectedStorage()
}
func (*publicationStore) Snapshot(context.Context, io.Writer) error { return unexpectedStorage() }
func (*publicationStore) Close() error                              { return unexpectedStorage() }
func unexpectedStorage() error {
	return errs.New(errs.KindInternal, "unexpected storage operation in native publication test")
}

func (store *publicationStore) remember() {
	if store.history == nil {
		store.history = make(map[int64]map[string]testkeyvalue.KeyValue)
	}
	entries := make(map[string]testkeyvalue.KeyValue, len(store.entries))
	for key := range store.entries {
		entries[key] = *store.entry(key)
	}
	store.history[store.revision] = entries
}
func (store *publicationStore) at(revision int64) (map[string]testkeyvalue.KeyValue, int64, error) {
	if revision == 0 || revision == store.revision {
		return store.entries, store.revision, nil
	}
	entries, found := store.history[revision]
	if !found {
		return nil, 0, unexpectedStorage()
	}
	return entries, revision, nil
}
