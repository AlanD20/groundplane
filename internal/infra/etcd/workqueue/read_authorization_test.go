package workqueue

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: reading captured configuration must not require a fake write, and
// a replaced executor, changed checkpoint or newly present stop fence must
// invalidate the entire read grant without changing execution state.
func TestReadAuthorizationRequiresCurrentOwnershipWithoutWrites(t *testing.T) {
	store := &readAuthorizationStore{
		assignmentStore: &assignmentStore{revision: 7, values: map[string]keyvalue.KeyValue{
			"/executor":   {Key: "/executor", ModRevision: 3, Value: []byte("executor")},
			"/checkpoint": {Key: "/checkpoint", ModRevision: 7, Value: []byte("checkpoint")},
		}},
	}
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	conditions := []keyvalue.Condition{
		{Key: "/executor", ModRevision: 3},
		{Key: "/checkpoint", ModRevision: 7},
		{Key: "/stop"},
	}
	current, err := repository.AuthorizeRead(context.Background(), conditions)
	if err != nil || !current || store.revision != 7 || len(store.values) != 2 {
		t.Fatalf("current read grant = %v/%v, revision %d", current, err, store.revision)
	}
	for _, key := range []string{"/executor", "/checkpoint", "/stop"} {
		original, existed := store.values[key]
		store.values[key] = keyvalue.KeyValue{Key: key, ModRevision: 8, Value: []byte("changed")}
		store.revision = 8
		current, err := repository.AuthorizeRead(context.Background(), conditions)
		if err != nil || current || store.revision != 8 {
			t.Fatalf("stale %s grant = %v/%v", key, current, err)
		}
		if existed {
			store.values[key] = original
		} else {
			delete(store.values, key)
		}
	}
	if _, err := repository.Commit(context.Background(), Change{Conditions: conditions}); err == nil {
		t.Fatal("read authorization weakened mutation-only Commit")
	}
}

type readAuthorizationStore struct{ *assignmentStore }

func (store *readAuthorizationStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	read := &keyvalue.GetManyResult{ReadRevision: store.revision, Values: make([]*keyvalue.KeyValue, len(request.Keys))}
	for index, key := range request.Keys {
		if value, exists := store.values[key]; exists {
			value.Value = append([]byte(nil), value.Value...)
			read.Values[index] = &value
		}
	}
	return read, nil
}
