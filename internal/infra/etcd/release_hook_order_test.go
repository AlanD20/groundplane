package etcd

import (
	"context"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

// Rationale: fixed-revision selection must retain captured order after a live
// metadata edit, preserve the slug tie-break, and never select unrelated hooks.
func TestPlanningHookOrderUsesFixedRevision(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	_, scripts := scriptBlueprintSeedActiveSet(t, store, 6)
	environmentID, serviceID := scripts[0].Record.EnvironmentID, scripts[0].Record.ServiceID
	var mutations []testkeyvalue.Mutation
	for index := range scripts {
		record := &scripts[index].Record
		if index < 5 {
			record.ServiceID = serviceID
		}
		record.Desired.When = core.ScriptPreDeploy
		record.Desired.Order = 10
		if index == 1 {
			record.Desired.Order = 0
		}
		if index == 3 {
			record.Desired.When = core.ScriptPreRollback
		}
		if index == 4 {
			record.Desired.When = core.ScriptManual
		}
		encoded, err := testscripts.EncodeRecord(*record)
		if err != nil {
			t.Fatal(err)
		}
		mutations = append(mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut,
			Key: testscripts.ScriptSetScriptKey(
				environmentID,
				record.ScriptSetGeneration,
				record.Desired.ID,
			), Value: encoded})
	}
	seeded, err := store.Transact(ctx, nil, mutations)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed ordered metadata: %#v, %v", seeded, err)
	}
	scope := testreleasequeries.ReleasePlanningScope{ReadRevision: seeded.Revision,
		Environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID},
		}}
	ledger := releaseLedgerFixture(t, &releasePlanningTestStore{memoryHierarchyStore: store})
	assertOrder := func(scope testreleasequeries.ReleasePlanningScope, operation domain.OperationKind, indexes ...int) {
		t.Helper()
		got, err := ledger.ListPlanningHookScriptIDs(ctx, scope, serviceID, operation)
		want := make([]string, len(indexes))
		for index, selected := range indexes {
			want[index] = scripts[selected].Record.Desired.ID
		}
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("selected = %v, %v; want %v", got, err, want)
		}
	}
	assertOrder(scope, domain.OperationDeploy, 1, 0, 2)
	assertOrder(scope, domain.OperationRollback, 3)
	record := scripts[1].Record
	record.Desired.Order = 65535
	encoded, err := testscripts.EncodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	edited, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
		Key: testscripts.ScriptSetScriptKey(
			environmentID,
			record.ScriptSetGeneration,
			record.Desired.ID,
		), Value: encoded}})
	if err != nil || !edited.Succeeded {
		t.Fatalf("edit order: %#v, %v", edited, err)
	}
	assertOrder(scope, domain.OperationDeploy, 1, 0, 2)
	scope.ReadRevision = edited.Revision
	assertOrder(scope, domain.OperationDeploy, 0, 2, 1)
}
