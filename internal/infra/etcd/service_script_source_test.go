package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: parent deletion cannot discard a Service source after private
// preparation, even though the Script Task has not been published yet.
func TestServiceParentFinalizerProtectsPreparedScriptSource(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	serviceID, environmentID := ids.New(ids.KindService), ids.New(ids.KindEnvironment)
	revision := seedServiceRepositoryTestRuntime(t, store, testservices.ServiceRuntimeRecord{
		EnvironmentID: environmentID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	member := testscriptsourceevidence.ScriptSourcePreparationMember{
		Reference: testscriptsourcereference.Reference{
			OperationID: ids.New(ids.KindOperation), ScriptExecutionID: ids.NewULID(),
			Source: testscriptsourcereference.SourceIdentity{
				Kind: testscriptsourcereference.SourceService, ServiceID: serviceID,
			}, SourceOwnerID: environmentID, SourceModRevision: revision,
		},
		Evidence: testscriptsourceevidence.ScriptSourceEvidence{
			Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{
				SourceKey: testservices.ServiceRuntimeKey(serviceID),
			},
		},
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	members := []testscriptsourceevidence.ScriptSourcePreparationMember{member}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	preparer := testhierarchydeletionfinalization.NewPreparer(store)
	before := store.revision
	action := testhierarchydeletion.HierarchyDeletionAction{
		TargetID: serviceID, TargetRevision: revision,
		ActionKind: testhierarchydeletion.HierarchyDeletionServiceRemove,
	}
	if _, err := preparer.Prepare(ctx, testhierarchydeletion.HierarchyDeletionOperation{}, action); !isKind(
		err,
		errs.KindResourceInUse,
	) ||
		store.revision != before {
		t.Fatalf("parent finalizer ignored reserved Service source: %v", err)
	}
	if err := authority.Abandon(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	effects, err := preparer.Prepare(ctx, testhierarchydeletion.HierarchyDeletionOperation{}, action)
	if err != nil {
		t.Fatal(err)
	}
	transactionStore := &connectorReferenceRaceStore{memoryHierarchyStore: store, injected: true}
	raceStore := &entryScriptReservationRaceStore{connectorReferenceRaceStore: transactionStore, member: member}
	race, err := raceStore.Transact(ctx, effects.Conditions(), effects.Mutations())
	if err != nil || race.Succeeded || !raceStore.reserved ||
		store.valueAt(testservices.ServiceRuntimeKey(serviceID), store.revision) == nil {
		t.Fatalf("Service finalizer lost source reservation race: %v", err)
	}
	if err := authority.Abandon(ctx, member.Reference.OperationID, members); err != nil {
		t.Fatal(err)
	}
	effects, err = preparer.Prepare(ctx, testhierarchydeletion.HierarchyDeletionOperation{}, action)
	if err != nil {
		t.Fatal(err)
	}
	result, err := transactionStore.Transact(ctx, effects.Conditions(), effects.Mutations())
	if err != nil || !result.Succeeded ||
		store.valueAt(testservices.ServiceRuntimeKey(serviceID), store.revision) != nil {
		t.Fatalf("unblocked Service finalizer did not finish: %v", err)
	}
}

// Rationale: a retained runtime sidecar cannot authorize new Script sources
// once Service removal owns host cleanup, before metadata is finalized.
func TestServiceScriptPreparationRejectsRetiringService(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	serviceID, environmentID := ids.New(ids.KindService), ids.New(ids.KindEnvironment)
	revision := seedServiceRepositoryTestRuntime(t, store, testservices.ServiceRuntimeRecord{
		EnvironmentID: environmentID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	member := testscriptsourceevidence.ScriptSourcePreparationMember{
		Reference: testscriptsourcereference.Reference{
			OperationID: ids.New(ids.KindOperation), ScriptExecutionID: ids.NewULID(),
			Source: testscriptsourcereference.SourceIdentity{
				Kind: testscriptsourcereference.SourceService, ServiceID: serviceID,
			}, SourceOwnerID: environmentID, SourceModRevision: revision,
		},
		Evidence: testscriptsourceevidence.ScriptSourceEvidence{
			Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{
				SourceKey: testservices.ServiceRuntimeKey(serviceID),
			},
		},
	}
	value, err := testdeletions.EncodeDeletionTombstone(testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetService, TargetID: serviceID, TargetRevision: revision,
		TaskID: ids.New(ids.KindTask), Phase: testdeletions.DeletionPhaseHostEffects,
		CreatedAt: serviceRecordTestTime(), UpdatedAt: serviceRecordTestTime(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testdeletions.TombstoneKey(string(testdeletions.DeletionTargetService), serviceID), Value: value,
	}}); err != nil {
		t.Fatal(err)
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Prepare(ctx, member.Reference.OperationID, []testscriptsourceevidence.ScriptSourcePreparationMember{member}); err == nil {
		t.Fatal("reserved a Service after retirement started")
	}
	if store.valueAt(testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source), store.revision) != nil {
		t.Fatal("rejected reservation left a Service count")
	}
}
