package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BACK-18: consumer admission must exclude destructive deletion even when an
// Attach appears after the deletion read but before its publication transaction.
func TestBackingDeletionExcludesExistingAndConcurrentConsumers(t *testing.T) {
	for _, mode := range []string{"unused", "connected", "concurrent"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			consumer := keyvalue.Mutation{Type: keyvalue.MutationPut,
				Key: attachments.AttachBackingProjectKey("backing", "consumer"), Value: []byte("consumer")}
			if mode == "connected" {
				if _, err := store.Transact(ctx, nil, []keyvalue.Mutation{consumer}); err != nil {
					t.Fatal(err)
				}
			}
			repository, err := newHierarchyDeletionRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			guards, err := repository.protectBackingDeletion(ctx,
				hierarchydeletion.HierarchyDeletionTargetBacking, "backing", store.revision)
			if mode == "connected" {
				if kind, ok := errs.KindOf(err); !ok || kind != errs.KindResourceInUse {
					t.Fatalf("connected instance admitted deletion: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "concurrent" {
				if _, err := store.Transact(ctx, nil, []keyvalue.Mutation{consumer}); err != nil {
					t.Fatal(err)
				}
			}
			result, err := store.Transact(ctx, guards, []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: "deletion-published", Value: []byte("yes")},
			})
			if err != nil || result.Succeeded != (mode == "unused") {
				t.Fatalf("deletion publication = %#v, %v", result, err)
			}
			if mode == "concurrent" {
				read, err := store.Get(ctx, "deletion-published")
				if err != nil || read.Entry != nil {
					t.Fatalf("racing consumer did not prevent deletion: %#v, %v", read, err)
				}
			}
		})
	}
}
