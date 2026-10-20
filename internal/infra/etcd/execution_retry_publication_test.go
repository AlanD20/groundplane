package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Rationale: Retry joins operation-owned input/recovery guards with execution
// guards, just as ordinary Commit does. Identical guards must not turn a valid
// request into500; conflicting captures must still reject without publication.
func TestExecutionRetryJoinsSharedOwnershipGuards(t *testing.T) {
	for _, revision := range []int64{0, 1} {
		store := newMemoryHierarchyStore()
		repository, err := NewIdempotencyRepository(store)
		if err != nil {
			t.Fatal(err)
		}
		marker := testDirectMarker()
		change := workqueue.Change{
			Conditions: []keyvalue.Condition{{Key: "/retry-input"}, {Key: "/retry-input", ModRevision: revision}},
			Mutations: []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: "/retry-result", Value: []byte("same-work-retry")},
			},
		}
		result, err := repository.ApplyExecutionRetry(context.Background(), change, marker)
		read, readErr := store.Get(context.Background(), "/retry-result")
		if readErr != nil {
			t.Fatal(readErr)
		}
		if revision != 0 {
			if err == nil || read.Entry != nil {
				t.Fatalf("conflicting Retry was published: %v", err)
			}
			continue
		}
		if err != nil || read.Entry == nil {
			t.Fatalf("shared guard Retry was not published: %v", err)
		}
		outcome, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
			t.Fatalf("Retry outcome %v/%v/%v", outcome, conflict, classifyErr)
		}
	}
}
