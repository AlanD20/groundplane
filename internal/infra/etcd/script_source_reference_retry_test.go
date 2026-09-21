package etcd

import (
	context "context"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	testing "testing"
)

func TestScriptSourceReferenceAuthorityExactRetryDoesNotIncrementCounts(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	members = append(members[:1:1], members[0])
	first, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare(first) error = %v", err)
	}
	countKey := testscriptsourceevidence.ScriptSourceCountKey(members[0].Reference.Source)
	countBefore := store.valueAt(countKey, store.revision)
	forwardBefore := store.valueAt(
		testscriptsourceevidence.ScriptSourceForwardReferenceKey(members[0].Reference),
		store.revision,
	)
	descriptorBefore := store.valueAt(testscriptsourcereference.PreparationKey(operationID), store.revision)
	second, err := authority.Prepare(context.Background(), operationID, members)
	if err != nil {
		t.Fatalf("Prepare(retry) error = %v", err)
	}
	countAfter := store.valueAt(countKey, store.revision)
	forwardAfter := store.valueAt(
		testscriptsourceevidence.ScriptSourceForwardReferenceKey(members[0].Reference),
		store.revision,
	)
	descriptorAfter := store.valueAt(testscriptsourcereference.PreparationKey(operationID), store.revision)
	if first.MembershipSHA256() != second.MembershipSHA256() ||
		descriptorBefore.ModRevision != descriptorAfter.ModRevision || descriptorBefore.Version != descriptorAfter.Version ||
		countBefore.ModRevision != countAfter.ModRevision || countBefore.Version != countAfter.Version ||
		forwardBefore.ModRevision != forwardAfter.ModRevision || forwardBefore.Version != forwardAfter.Version {
		t.Fatal("exact retry mutated durable source references")
	}
}

func TestScriptSourceReferenceAuthorityFailedFinalCASHasNoActiveRoot(t *testing.T) {
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	prepared, err := authority.Prepare(context.Background(), operationID, members[:1])
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	fragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	defer fragment.Clear()
	descriptor := store.valueAt(testscriptsourcereference.PreparationKey(operationID), store.revision)
	raced, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: descriptor.Key, ModRevision: descriptor.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: descriptor.Key, Value: descriptor.Value}},
	)
	if err != nil || !raced.Succeeded {
		t.Fatalf("race descriptor = %#v, %v", raced, err)
	}
	result, err := store.Transact(context.Background(), fragment.Conditions(), fragment.Mutations())
	if err != nil || result.Succeeded ||
		store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(operationID), store.revision) != nil {
		t.Fatalf("stale final CAS = %#v, %v; root became visible", result, err)
	}
}
