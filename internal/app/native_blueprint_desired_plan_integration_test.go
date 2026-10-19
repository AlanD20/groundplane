package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Actual current desired-plan storage; prerequisite hook effects and scheduler
// ownership are supplied, not qualified by this metadata expansion check.
func TestNativeBlueprintDesiredPlanSealsAfterFactDependentExpansion(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 70)
	parentID := ids.NewAt(ids.KindTask, now, 71)
	headValue, err := idempotencyrecord.EncodeTaskReference(parentID)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(environmentID), Value: headValue,
	}}); err != nil || !tx.Succeeded {
		t.Fatalf("seed desired head = %#v, %v", tx, err)
	}
	attach := blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: ids.NewAt(ids.KindAttach, now, 72)}
	service := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 73)}
	seedNativeBlueprintInitialAbsence(t, store, environmentID, parentID, attach, service)
	initial, err := ledger.Load(ctx, environmentID)
	if err != nil {
		t.Fatal(err)
	}
	first := blueprintunits.DesiredPlan{
		EnvironmentID: environmentID, ParentTaskID: parentID,
		Units: []blueprintunits.Unit{
			{Target: attach, Writes: []blueprintunits.ResourceKey{attach}, Fingerprint: strings.Repeat("a", 64)},
		},
	}
	prepared, err := blueprintunits.PrepareDesiredPlan(initial, first)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Clear()
	if tx, err := store.Transact(ctx, prepared.Conditions(), prepared.Mutations()); err != nil || !tx.Succeeded {
		t.Fatalf("publish incomplete plan = %#v, %v", tx, err)
	}
	loaded, err := ledger.Load(ctx, environmentID)
	if err != nil || loaded.Desired == nil || loaded.Desired.Record.Complete || len(loaded.Desired.Record.Units) != 1 {
		t.Fatalf("incomplete plan = %#v, %v", loaded.Desired, err)
	}
	final := first
	final.Complete = true
	final.Units = append(append([]blueprintunits.Unit(nil), first.Units...), blueprintunits.Unit{
		Target: service, Writes: []blueprintunits.ResourceKey{service}, Fingerprint: strings.Repeat("b", 64),
		After: []blueprintunits.ResourceKey{attach},
	})
	sealed, err := blueprintunits.PrepareDesiredPlan(loaded, final)
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Clear()
	// Independently advance each source revision after preparation. Even an
	// identical value invalidates the old decision; neither race may publish
	// the expanded plan before it is prepared from a fresh snapshot.
	for _, key := range []string{blueprints.EnvironmentBlueprintHeadKey(environmentID), blueprintunits.EpochKey(environmentID)} {
		before, err := store.Get(ctx, key)
		if err != nil || before.Entry == nil {
			t.Fatalf("plan source unavailable: %v", err)
		}
		if _, err := store.Put(ctx, key, before.Entry.Value); err != nil {
			t.Fatal(err)
		}
		clear(before.Entry.Value)
		if tx, err := store.Transact(ctx, sealed.Conditions(), sealed.Mutations()); err != nil || tx.Succeeded {
			t.Fatalf("plan source race committed stale expansion: %v", err)
		}
		fresh, err := ledger.Load(ctx, environmentID)
		if err != nil || fresh.Desired == nil || fresh.Desired.Record.Complete || len(fresh.Desired.Record.Units) != 1 {
			t.Fatalf("source race partially sealed the plan: %v", err)
		}
		sealed.Clear()
		sealed, err = blueprintunits.PrepareDesiredPlan(fresh, final)
		if err != nil {
			t.Fatal(err)
		}
		defer sealed.Clear()
	}
	if tx, err := store.Transact(ctx, sealed.Conditions(), sealed.Mutations()); err != nil || !tx.Succeeded {
		t.Fatalf("seal expanded plan = %#v, %v", tx, err)
	}
	if tx, err := store.Transact(ctx, prepared.Conditions(), prepared.Mutations()); err != nil || tx.Succeeded {
		t.Fatalf("stale plan overwrote expansion = %#v, %v", tx, err)
	}
	current, err := ledger.Load(ctx, environmentID)
	if err != nil || current.Desired == nil || !current.Desired.Record.Complete ||
		len(current.Desired.Record.Units) != 2 {
		t.Fatalf("sealed plan = %#v, %v", current.Desired, err)
	}
	if _, err := blueprintunits.PrepareDesiredPlan(current, first); err == nil {
		t.Fatal("sealed plan was reopened")
	}
}

func seedNativeBlueprintInitialAbsence(
	t *testing.T, store *memoryHierarchyStore, environmentID, parentID string, targets ...blueprintunits.ResourceKey,
) {
	t.Helper()
	publication, err := blueprintunits.PrepareInitialAbsencePublication(environmentID, parentID, targets)
	if err != nil {
		t.Fatal(err)
	}
	defer publication.Clear()
	if result, err := store.Transact(context.Background(), publication.Conditions(), publication.Mutations()); err != nil ||
		!result.Succeeded {
		t.Fatalf("seed unit initial absence = %#v, %v", result, err)
	}
}
