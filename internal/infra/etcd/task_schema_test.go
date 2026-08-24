package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskJournalSchemaGateInitializesOnlyAnEmptyTaskCollection(t *testing.T) {
	// Rationale: a clean store may acquire the journal schema marker exactly once and every restart must accept it.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if err := repository.EnsureTaskJournalSchema(ctx); err != nil {
		t.Fatalf("EnsureTaskJournalSchema(empty) error = %v", err)
	}
	marker, err := store.Get(ctx, taskJournalSchemaKey)
	if err != nil || marker.Entry == nil || string(marker.Entry.Value) != taskJournalSchemaValue {
		t.Fatalf("schema marker = %#v, %v", marker, err)
	}
	if err := repository.EnsureTaskJournalSchema(ctx); err != nil {
		t.Fatalf("EnsureTaskJournalSchema(restart) error = %v", err)
	}
}

func TestTaskJournalSchemaGateRejectsLegacyMalformedAndFailedCAS(t *testing.T) {
	// Rationale: an existing Task or incompatible marker must fail closed instead of silently claiming the current schema.
	tests := map[string]func(*testing.T, *memoryTaskStore) taskRepositoryStore{
		"legacy tasks": func(t *testing.T, store *memoryTaskStore) taskRepositoryStore {
			seedTaskRepositoryValue(t, store, taskKey(ids.New(ids.KindTask)), []byte("legacy"))
			return store
		},
		"malformed marker": func(t *testing.T, store *memoryTaskStore) taskRepositoryStore {
			seedTaskRepositoryValue(t, store, taskJournalSchemaKey, []byte("not-v1"))
			return store
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(prepare(t, store))
			if err != nil {
				t.Fatalf("newTaskRepository() error = %v", err)
			}
			if err := repository.EnsureTaskJournalSchema(context.Background()); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("EnsureTaskJournalSchema() error = %v, want internal", err)
			}
		})
	}
}

func TestTaskJournalSchemaGateRejectsTaskCreatedDuringInitialization(t *testing.T) {
	// Rationale: the schema marker CAS must prove that no Task appeared after the gate's empty fixed-revision read.
	ctx := context.Background()
	store := &taskSchemaRaceStore{
		memoryTaskStore: newMemoryTaskStore(),
		transacting:     make(chan struct{}),
		proceed:         make(chan struct{}),
	}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- repository.EnsureTaskJournalSchema(ctx) }()
	<-store.transacting
	seedTaskRepositoryValue(t, store.memoryTaskStore, taskKey(ids.New(ids.KindTask)), []byte("racing-task"))
	close(store.proceed)
	if err := <-result; !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("EnsureTaskJournalSchema(racing Task) error = %v, want internal", err)
	}
	marker, err := store.Get(ctx, taskJournalSchemaKey)
	if err != nil || marker.Entry != nil {
		t.Fatalf("schema marker after racing Task = %#v, %v", marker, err)
	}
}

type taskSchemaRaceStore struct {
	*memoryTaskStore
	transacting chan struct{}
	proceed     chan struct{}
}

func (store *taskSchemaRaceStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	close(store.transacting)
	select {
	case <-store.proceed:
	case <-ctx.Done():
		return TransactionResult{}, ctx.Err()
	}
	return store.memoryTaskStore.Transact(ctx, conditions, mutations)
}
