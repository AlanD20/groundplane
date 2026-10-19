package etcd

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Shared key/value test storage retained independently of the retired Task
// engine. It implements storage/CAS/watch behavior, not execution APIs.
func seedTaskRepositoryValue(t *testing.T, store *memoryTaskStore, key string, value []byte) {
	t.Helper()
	if _, err := store.Transact(context.Background(), []testkeyvalue.Condition{{Key: key}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: key, Value: value,
	}}); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

type memoryTaskVersion struct {
	revision int64
	value    []byte
	present  bool
}

type memoryTaskStore struct {
	mu                  sync.Mutex
	revision            int64
	history             map[string][]memoryTaskVersion
	failAfterCommitOnce error
	conflictsRemaining  int
	nextWatcherID       int
	watchers            map[int]*memoryTaskWatcher
	watchStarts         []memoryTaskWatchStart
}

type memoryTaskWatcher struct {
	prefix        string
	startRevision int64
	events        chan testkeyvalue.Event
	errors        chan error
}

type memoryTaskWatchStart struct {
	prefix   string
	revision int64
}

func newMemoryTaskStore() *memoryTaskStore {
	return &memoryTaskStore{history: make(map[string][]memoryTaskVersion)}
}

func (store *memoryTaskStore) failAfterCommit(err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.failAfterCommitOnce = err
}

func (store *memoryTaskStore) conflictNextTransactions(count int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.conflictsRemaining = count
}

func (store *memoryTaskStore) currentRevision() int64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.revision
}

func (store *memoryTaskStore) Get(ctx context.Context, key string) (*testkeyvalue.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return &testkeyvalue.GetResult{Entry: store.valueAtLocked(key, store.revision), ReadRevision: store.revision}, nil
}

func (store *memoryTaskStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueAtLocked(key, revision)
	}
	return &testkeyvalue.GetManyResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision,
	}, nil
}

func (store *memoryTaskStore) Watch(
	ctx context.Context,
	prefix string,
	startRevision int64,
) (*testkeyvalue.WatchStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	if startRevision == 0 {
		startRevision = store.revision + 1
	}
	events := make(chan testkeyvalue.Event, 4096)
	errorsFound := make(chan error, 1)
	history := make([]testkeyvalue.Event, 0)
	for key, versions := range store.history {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, version := range versions {
			if version.revision < startRevision {
				continue
			}
			event := testkeyvalue.Event{Key: key, ModRevision: version.revision, Type: testkeyvalue.EventDelete}
			if version.present {
				event.Type = testkeyvalue.EventPut
				event.Value = append([]byte(nil), version.value...)
			}
			history = append(history, event)
		}
	}
	sort.Slice(history, func(left int, right int) bool {
		if history[left].ModRevision != history[right].ModRevision {
			return history[left].ModRevision < history[right].ModRevision
		}
		return history[left].Key < history[right].Key
	})
	for _, event := range history {
		events <- event
	}
	store.nextWatcherID++
	id := store.nextWatcherID
	if store.watchers == nil {
		store.watchers = make(map[int]*memoryTaskWatcher)
	}
	store.watchers[id] = &memoryTaskWatcher{
		prefix: prefix, startRevision: startRevision, events: events, errors: errorsFound,
	}
	store.watchStarts = append(store.watchStarts, memoryTaskWatchStart{prefix: prefix, revision: startRevision})
	store.mu.Unlock()

	go func() {
		<-ctx.Done()
		store.mu.Lock()
		watcher, exists := store.watchers[id]
		if exists {
			delete(store.watchers, id)
			close(watcher.errors)
			close(watcher.events)
		}
		store.mu.Unlock()
	}()
	return &testkeyvalue.WatchStream{Events: events, Errors: errorsFound}, nil
}

func (store *memoryTaskStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.conflictsRemaining > 0 {
		store.conflictsRemaining--
		return testkeyvalue.TransactionResult{Revision: store.revision}, nil
	}
	for _, condition := range conditions {
		value := store.conditionValueLocked(condition, store.revision)
		actual := int64(0)
		if value != nil {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
			for index, failedCondition := range conditions {
				failureReads[index] = store.conditionValueLocked(failedCondition, store.revision)
			}
			return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: failureReads}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		version := memoryTaskVersion{revision: store.revision}
		event := testkeyvalue.Event{Key: mutation.Key, ModRevision: store.revision, Type: testkeyvalue.EventDelete}
		switch mutation.Type {
		case testkeyvalue.MutationPut:
			version.present = true
			version.value = append([]byte(nil), mutation.Value...)
			event.Type = testkeyvalue.EventPut
			event.Value = append([]byte(nil), mutation.Value...)
		case testkeyvalue.MutationDelete:
		default:
			return testkeyvalue.TransactionResult{}, errs.New(
				errs.KindInternal,
				"fake task store received invalid mutation",
			)
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], version)
		for _, watcher := range store.watchers {
			if store.revision >= watcher.startRevision && strings.HasPrefix(mutation.Key, watcher.prefix) {
				watcher.events <- event
			}
		}
	}
	if store.failAfterCommitOnce != nil {
		err := store.failAfterCommitOnce
		store.failAfterCommitOnce = nil
		return testkeyvalue.TransactionResult{}, err
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryTaskStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *memoryTaskStore) conditionValueLocked(
	condition testkeyvalue.Condition,
	revision int64,
) *testkeyvalue.KeyValue {
	if !condition.Prefix {
		return store.valueAtLocked(condition.Key, revision)
	}
	var first *testkeyvalue.KeyValue
	for key := range store.history {
		if !strings.HasPrefix(key, condition.Key) {
			continue
		}
		value := store.valueAtLocked(key, revision)
		if value != nil && (first == nil || value.Key < first.Key) {
			first = value
		}
	}
	return first
}

func (store *memoryTaskStore) failWatch(prefix string, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for id, watcher := range store.watchers {
		if watcher.prefix != prefix {
			continue
		}
		delete(store.watchers, id)
		watcher.errors <- err
		close(watcher.errors)
		close(watcher.events)
	}
}

func (store *memoryTaskStore) watcherCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.watchers)
}

func (store *memoryTaskStore) watchStartCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.watchStarts)
}

func (store *memoryTaskStore) valueAtLocked(key string, revision int64) *testkeyvalue.KeyValue {
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
