package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a Script reservation after the removal read must be detected by
// the actual final transaction, including when the old generation is reserved.
func TestEntryRemovalFencesScriptReservationAfterRead(t *testing.T) {
	for _, path := range []string{"dispatch", "direct-finalizer", "parent-finalizer"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			_, memory, environment, project, current, generationIDs := entryDeletionTestState(t)
			member := entryScriptTestMember(t, memory, current.Record, generationIDs[0])
			store := &entryScriptReservationRaceStore{
				connectorReferenceRaceStore: &connectorReferenceRaceStore{memoryHierarchyStore: memory, injected: true},
				member:                      member,
			}
			entries, err := newEntryRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			switch path {
			case "dispatch":
				task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
				result, err := entries.BeginEntryDeletionWithTask(
					ctx, environment, project, current, nil, tombstone, intent, task, marker,
				)
				if err != nil || !isKind(result.conflict, errs.KindResourceInUse) {
					t.Fatalf("dispatch lost reservation race: %v / %v", err, result.conflict)
				}
				if memory.valueAt(taskKey(task.ID), memory.revision) != nil {
					t.Fatal("losing deletion published a Task")
				}
			case "direct-finalizer":
				if _, err := entries.DeleteEntry(ctx, environment, project, current); !isKind(
					err,
					errs.KindResourceInUse,
				) {
					t.Fatalf("direct finalizer lost reservation race: %v", err)
				}
			case "parent-finalizer":
				hierarchy, err := newHierarchyDeletionRepository(store)
				if err != nil {
					t.Fatal(err)
				}
				effects, err := hierarchy.prepareHierarchyDeletionEntryFinalizer(ctx, HierarchyDeletionAction{
					TargetID: current.Record.Entry.ID, TargetRevision: current.Revision,
				})
				if err != nil {
					t.Fatal(err)
				}
				result, err := store.Transact(ctx, effects.conditions, effects.mutations)
				if err != nil || result.Succeeded {
					t.Fatalf("parent finalizer lost reservation race: %v", err)
				}
			}
			if !store.reserved {
				t.Fatal("final transaction did not compare Entry source absence")
			}
			assertEntryRemovalRetained(t, entries, memory, current, generationIDs)
			if memory.valueAt(scriptSourceCountKey(member.Reference.Source), memory.revision) == nil ||
				memory.valueAt(scriptSourceForwardReferenceKey(member.Reference), memory.revision) == nil {
				t.Fatal("losing deletion damaged the winning Script reservation")
			}
		})
	}
}

type entryScriptReservationRaceStore struct {
	*connectorReferenceRaceStore
	member   ScriptSourcePreparationMember
	reserved bool
}

func (store *entryScriptReservationRaceStore) Transact(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	if !store.reserved &&
		connectorReferenceConditionContains(conditions, scriptSourceForwardReferenceKey(store.member.Reference)) {
		authority, err := newScriptSourceReferenceAuthority(store.memoryHierarchyStore)
		if err != nil {
			return TransactionResult{}, err
		}
		if _, err := authority.Prepare(ctx, store.member.Reference.OperationID,
			[]ScriptSourcePreparationMember{store.member}); err != nil {
			return TransactionResult{}, err
		}
		store.reserved = true
	}
	return store.connectorReferenceRaceStore.Transact(ctx, conditions, mutations)
}
