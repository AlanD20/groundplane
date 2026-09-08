package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: parent deletion cannot discard a Service source after private
// preparation, even though the Script Task has not been published yet.
func TestServiceParentFinalizerProtectsPreparedScriptSource(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	serviceID, environmentID := ids.New(ids.KindService), ids.New(ids.KindEnvironment)
	revision := seedServiceRepositoryTestRuntime(t, store, ServiceRuntimeRecord{
		EnvironmentID: environmentID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	member := ScriptSourcePreparationMember{
		Reference: ScriptSourceReference{
			OperationID: ids.New(ids.KindOperation), ScriptExecutionID: ids.NewULID(),
			Source: serviceScriptSource(serviceID), SourceOwnerID: environmentID, SourceModRevision: revision,
		},
		Evidence: ScriptSourceEvidence{
			Existing: &ScriptExistingSourceEvidence{SourceKey: serviceRuntimeKey(serviceID)},
		},
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	members := []ScriptSourcePreparationMember{member}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	repository, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	before := store.revision
	action := HierarchyDeletionAction{TargetID: serviceID, TargetRevision: revision}
	if _, err := repository.prepareHierarchyDeletionServiceFinalizer(ctx, action); !isKind(
		err,
		errs.KindResourceInUse,
	) ||
		store.revision != before {
		t.Fatalf("parent finalizer ignored reserved Service source: %v", err)
	}
	if err := authority.Abandon(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	effects, err := repository.prepareHierarchyDeletionServiceFinalizer(ctx, action)
	if err != nil {
		t.Fatal(err)
	}
	transactionStore := &connectorReferenceRaceStore{memoryHierarchyStore: store, injected: true}
	raceStore := &entryScriptReservationRaceStore{connectorReferenceRaceStore: transactionStore, member: member}
	race, err := raceStore.Transact(ctx, effects.conditions, effects.mutations)
	if err != nil || race.Succeeded || !raceStore.reserved ||
		store.valueAt(serviceRuntimeKey(serviceID), store.revision) == nil {
		t.Fatalf("Service finalizer lost source reservation race: %v", err)
	}
	if err := authority.Abandon(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	effects, err = repository.prepareHierarchyDeletionServiceFinalizer(ctx, action)
	if err != nil {
		t.Fatal(err)
	}
	result, err := transactionStore.Transact(ctx, effects.conditions, effects.mutations)
	if err != nil || !result.Succeeded || store.valueAt(serviceRuntimeKey(serviceID), store.revision) != nil {
		t.Fatalf("unblocked Service finalizer did not finish: %v", err)
	}
}

// Rationale: a retained runtime sidecar cannot authorize new Script sources
// once Service removal owns host cleanup, before metadata is finalized.
func TestServiceScriptPreparationRejectsRetiringService(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	serviceID, environmentID := ids.New(ids.KindService), ids.New(ids.KindEnvironment)
	revision := seedServiceRepositoryTestRuntime(t, store, ServiceRuntimeRecord{
		EnvironmentID: environmentID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	member := ScriptSourcePreparationMember{
		Reference: ScriptSourceReference{
			OperationID: ids.New(ids.KindOperation), ScriptExecutionID: ids.NewULID(),
			Source: serviceScriptSource(serviceID), SourceOwnerID: environmentID, SourceModRevision: revision,
		},
		Evidence: ScriptSourceEvidence{
			Existing: &ScriptExistingSourceEvidence{SourceKey: serviceRuntimeKey(serviceID)},
		},
	}
	value, err := encodeDeletionTombstone(DeletionTombstoneRecord{
		TargetKind: DeletionTargetService, TargetID: serviceID, TargetRevision: revision,
		TaskID: ids.New(ids.KindTask), Phase: DeletionPhaseHostEffects,
		CreatedAt: serviceRecordTestTime(), UpdatedAt: serviceRecordTestTime(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetService), serviceID), Value: value,
	}}); err != nil {
		t.Fatal(err)
	}
	authority, err := newScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, []ScriptSourcePreparationMember{member}); err == nil {
		t.Fatal("reserved a Service after retirement started")
	}
	if store.valueAt(scriptSourceCountKey(member.Reference.Source), store.revision) != nil {
		t.Fatal("rejected reservation left a Service count")
	}
}
