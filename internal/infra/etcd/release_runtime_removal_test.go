package etcd

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

type rejectEmptyHierarchyMultiGetStore struct {
	hierarchyDeletionStore
}

func (store rejectEmptyHierarchyMultiGetStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if len(request.Keys) == 0 {
		return nil, errors.New("empty multi-get")
	}
	return store.hierarchyDeletionStore.GetMany(ctx, request)
}

// Rationale: ancestor deletion uses a separate Service finalizer. Removing its
// runtime metadata must remove the new current receipt in the same transaction
// while preserving another Service's receipt and immutable Release history.
func TestReleaseRuntimeReceiptRemovedWithHierarchyService(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newReleaseTerminalFixture(t, 2)
	if processed, err := f.finalize(t, testtaskjournal.TaskStatusCompleted); err != nil || !processed {
		t.Fatalf("publish runtime receipts: %v", err)
	}
	member, other := f.head.Members[0], f.head.Members[1]
	otherBefore, err := f.store.Get(ctx, serviceruntimerecord.Key(other.ServiceID))
	if err != nil || otherBefore.Entry == nil {
		t.Fatal("other receipt absent")
	}
	value, err := testservices.EncodeServiceRuntimeRecord(testservices.ServiceRuntimeRecord{
		EnvironmentID: f.head.EnvironmentID, ServiceID: member.ServiceID,
		Runtime: core.ServiceRuntime{ServiceID: member.ServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := f.store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(member.ServiceID), Value: value},
		},
	)
	if err != nil || !seed.Succeeded {
		t.Fatal("seed Service runtime")
	}
	store := rejectEmptyHierarchyMultiGetStore{
		hierarchyDeletionStore: f.store,
	}
	effects, err := testhierarchydeletionfinalization.NewPreparer(store).Prepare(
		ctx, testhierarchydeletion.HierarchyDeletionOperation{}, testhierarchydeletion.HierarchyDeletionAction{
			TargetID: member.ServiceID, TargetRevision: seed.Revision,
			ActionKind: testhierarchydeletion.HierarchyDeletionServiceRemove,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer testkeyvalue.ClearByteSlices(effects.Values())
	removed, err := f.store.Transact(ctx, effects.Conditions(), effects.Mutations())
	if err != nil || !removed.Succeeded {
		t.Fatalf("Service finalization: %v", err)
	}
	for _, key := range []string{testservices.ServiceRuntimeKey(member.ServiceID), serviceruntimerecord.Key(member.ServiceID)} {
		got, err := f.store.Get(ctx, key)
		if err != nil || got.Entry != nil {
			t.Fatal("Service finalization retained removed Service runtime")
		}
	}
	otherAfter, err := f.store.Get(ctx, serviceruntimerecord.Key(other.ServiceID))
	if err != nil || otherAfter.Entry == nil || !bytes.Equal(otherAfter.Entry.Value, otherBefore.Entry.Value) {
		t.Fatal("Service finalization changed unrelated receipt")
	}
	history, err := f.store.Get(ctx, testreleases.ReleaseTerminalKey(member.ReleaseID))
	if err != nil || history.Entry == nil {
		t.Fatal("Service finalization removed immutable Release history")
	}
}
