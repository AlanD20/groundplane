package entryvalues

import (
	"bytes"
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type generationStore struct {
	mu       sync.Mutex
	revision int64
	values   map[string]keyvalue.KeyValue
}

func newGenerationStore() *generationStore {
	return &generationStore{values: make(map[string]keyvalue.KeyValue)}
}

func (store *generationStore) Get(ctx context.Context, key string) (*keyvalue.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := &keyvalue.GetResult{ReadRevision: store.revision}
	if value, found := store.values[key]; found {
		value.Value = bytes.Clone(value.Value)
		result.Entry = &value
	}
	return result, nil
}

func (store *generationStore) Transact(
	ctx context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return keyvalue.TransactionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, condition := range conditions {
		if condition.Prefix {
			return keyvalue.TransactionResult{}, errs.New(errs.KindInternal, "unsupported fixture prefix comparison")
		}
		if store.values[condition.Key].ModRevision != condition.ModRevision {
			return keyvalue.TransactionResult{Revision: store.revision}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		if mutation.Type != keyvalue.MutationPut || mutation.Prefix {
			return keyvalue.TransactionResult{}, errs.New(errs.KindInternal, "unsupported fixture mutation")
		}
		store.values[mutation.Key] = keyvalue.KeyValue{
			Key: mutation.Key, Value: bytes.Clone(mutation.Value), ModRevision: store.revision,
		}
	}
	return keyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}
