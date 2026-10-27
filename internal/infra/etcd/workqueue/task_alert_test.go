package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskalerts"
	"testing"
	"time"
)

// ALERT-03: competing Task updates must not publish a notification for a lost
// transition. The Task and alert use the same CAS transaction.
func TestTaskAlertCannotOutliveRejectedTransition(t *testing.T) {
	store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	fixture := assignmentFixture(t, store, workexecution.Release, nil)
	before := fixture.Task
	next := before.Task
	next.Progress = workexecution.Progress{Attention: 1}
	next.UpdatedAt = next.UpdatedAt.Add(time.Second)
	change, err := taskChange(before, next)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Transact(t.Context(), change.Conditions, change.Mutations)
	if err != nil || !first.Succeeded {
		t.Fatal("first transition failed", err)
	}
	duplicate, err := taskChange(before, next)
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := store.Transact(t.Context(), duplicate.Conditions, duplicate.Mutations)
	if err != nil || rejected.Succeeded {
		t.Fatal("stale transition was accepted", err)
	}
	count := 0
	for key := range store.values {
		if len(key) >= len(taskalerts.OutboxPrefix) && key[:len(taskalerts.OutboxPrefix)] == taskalerts.OutboxPrefix {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("durable alerts = %d, want one", count)
	}
	keyvalue.ClearMutationValues(change.Mutations)
	keyvalue.ClearMutationValues(duplicate.Mutations)
}
