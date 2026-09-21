package etcd

import (
	"context"
	"errors"
	"testing"

	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: manual Run must publish a Task when the Service and desired Zone
// projection share the same Environment head, while retaining its CAS fence.
func TestManualScriptSourceConditionsShareDesiredHead(t *testing.T) {
	_, sources := manualScriptSourceTestFixture(t)
	conditions, err := manualScriptSourceConditions(sources)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateIdempotencyPlanKeys(conditions, nil); err != nil {
		t.Fatalf("manual publication source conditions: %v", err)
	}
	if len(conditions) != 2 {
		t.Fatalf("source fences = %d, want head and immutable root", len(conditions))
	}
	store := newMemoryTaskStore()
	ctx := context.Background()
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: conditions[0].Key, Value: []byte("head")},
		{Type: testkeyvalue.MutationPut, Key: conditions[1].Key, Value: []byte("root")},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed sources = %#v, %v", seed, err)
	}
	for index := range conditions {
		conditions[index].ModRevision = seed.Revision
	}
	result, err := store.Transact(ctx, conditions, nil)
	if err != nil || !result.Succeeded {
		t.Fatalf("unchanged sources = %#v, %v", result, err)
	}
	changed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: conditions[0].Key, Value: []byte("changed")},
	})
	if err != nil || !changed.Succeeded {
		t.Fatalf("change head = %#v, %v", changed, err)
	}
	result, err = store.Transact(ctx, conditions, nil)
	if err != nil || result.Succeeded {
		t.Fatalf("stale source head = %#v, %v", result, err)
	}
}

// Rationale: overlapping sources may share a compare only when both readers
// captured the same revision; coalescing must never choose one conflicting view.
func TestManualScriptSourceConditionsRejectConflictingHead(t *testing.T) {
	_, sources := manualScriptSourceTestFixture(t)
	sources.Service.Revision++
	conditions, err := manualScriptSourceConditions(sources)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || conditions != nil {
		t.Fatalf("conflicting source revisions = %#v, %v", conditions, err)
	}
}

// Rationale: a separately pinned Service root remains an independent source
// fence and must not disappear when the current Environment head is compared.
func TestManualScriptSourceConditionsPreserveIndependentServiceFence(t *testing.T) {
	store, sources := manualScriptSourceTestFixture(t)
	pinnedKey := testblueprints.EnvironmentBlueprintRootKey(sources.Environment.Record.ID, "pinned")
	service, err := testservices.ReadJoined(
		context.Background(),
		store,
		testservices.DesiredSelection{
			Services: []testservices.EnvironmentServiceProjection{{
				EnvironmentID: sources.Service.Record.EnvironmentID,
				Desired:       sources.Service.Record.Desired,
			}},
			Revision: sources.Service.Revision, ReadRevision: sources.Service.ReadRevision,
		},
		sources.Service.Record.Desired.ID,
		pinnedKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	sources.Service = service
	conditions, err := manualScriptSourceConditions(sources)
	if err != nil || len(conditions) != 3 {
		t.Fatalf("independent source fences = %#v, %v", conditions, err)
	}
	if conditions[0] != testservices.ServiceDesiredCondition(sources.Service) {
		t.Fatalf("Service fence = %#v", conditions[0])
	}
	if err := validateIdempotencyPlanKeys(conditions, nil); err != nil {
		t.Fatal(err)
	}
}

func manualScriptSourceTestFixture(
	t *testing.T,
) (*memoryHierarchyStore, testscriptsourcequeries.ScriptExecutionSources) {
	t.Helper()
	store, sources, _, _, _ := manualScriptLifecycleFixture(t)
	return store, sources
}
