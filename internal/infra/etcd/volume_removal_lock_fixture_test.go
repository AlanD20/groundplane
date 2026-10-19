package etcd

import (
	"context"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

func putSpecializedRemovalLock(t *testing.T, store *memoryHierarchyStore, owner removalrecord.Owner) {
	t.Helper()
	value, err := removalrecord.EncodeOwner(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
		Key: removalrecord.EnvironmentLockKey(owner.EnvironmentID), Value: value}}); err != nil {
		t.Fatal(err)
	}
}

type specializedRemovalLockRaceStore struct {
	*memoryHierarchyStore
	owner *removalrecord.Owner
}

func (store *specializedRemovalLockRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if store.owner != nil {
		owner := *store.owner
		value, err := removalrecord.EncodeOwner(owner)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
			Key: removalrecord.EnvironmentLockKey(owner.EnvironmentID), Value: value}}); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		store.owner = nil
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
