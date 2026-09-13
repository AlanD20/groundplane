package etcd

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// Rationale: ancestor deletion uses a separate Service finalizer. Removing its
// runtime metadata must remove the new current receipt in the same transaction
// while preserving another Service's receipt and immutable Release history.
func TestReleaseRuntimeReceiptRemovedWithHierarchyService(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newReleaseTerminalFixture(t, 2)
	if processed, err := f.finalize(t, TaskStatusCompleted); err != nil || !processed {
		t.Fatalf("publish runtime receipts: %v", err)
	}
	member, other := f.head.Members[0], f.head.Members[1]
	otherBefore, err := f.store.Get(ctx, serviceruntimerecord.Key(other.ServiceID))
	if err != nil || otherBefore.Entry == nil {
		t.Fatal("other receipt absent")
	}
	value, err := encodeServiceRuntimeRecord(ServiceRuntimeRecord{
		EnvironmentID: f.head.EnvironmentID, ServiceID: member.ServiceID,
		Runtime: core.ServiceRuntime{ServiceID: member.ServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := f.store.Transact(
		ctx,
		nil,
		[]Mutation{{Type: MutationPut, Key: serviceRuntimeKey(member.ServiceID), Value: value}},
	)
	if err != nil || !seed.Succeeded {
		t.Fatal("seed Service runtime")
	}
	repository := &HierarchyDeletionRepository{store: f.store}
	effects, err := repository.prepareHierarchyDeletionServiceFinalizer(ctx, HierarchyDeletionAction{
		TargetID: member.ServiceID, TargetRevision: seed.Revision, ActionKind: HierarchyDeletionServiceRemove,
	})
	if err != nil {
		t.Fatal(err)
	}
	removed, err := f.store.Transact(ctx, effects.conditions, effects.mutations)
	if err != nil || !removed.Succeeded {
		t.Fatalf("Service finalization: %v", err)
	}
	for _, key := range []string{serviceRuntimeKey(member.ServiceID), serviceruntimerecord.Key(member.ServiceID)} {
		got, err := f.store.Get(ctx, key)
		if err != nil || got.Entry != nil {
			t.Fatal("Service finalization retained removed Service runtime")
		}
	}
	otherAfter, err := f.store.Get(ctx, serviceruntimerecord.Key(other.ServiceID))
	if err != nil || otherAfter.Entry == nil || !bytes.Equal(otherAfter.Entry.Value, otherBefore.Entry.Value) {
		t.Fatal("Service finalization changed unrelated receipt")
	}
	history, err := f.store.Get(ctx, releaseTerminalKey(member.ReleaseID))
	if err != nil || history.Entry == nil {
		t.Fatal("Service finalization removed immutable Release history")
	}
}
