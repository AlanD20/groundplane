package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: manual Run must publish a Task when the Service and desired Zone
// projection share the same Environment head, while retaining its CAS fence.
func TestManualScriptSourceConditionsShareDesiredHead(t *testing.T) {
	sources := manualScriptSourceTestFixture()
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
	seed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: conditions[0].Key, Value: []byte("head")},
		{Type: MutationPut, Key: conditions[1].Key, Value: []byte("root")},
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
	changed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: conditions[0].Key, Value: []byte("changed")},
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
	sources := manualScriptSourceTestFixture()
	sources.Service.Revision++
	conditions, err := manualScriptSourceConditions(sources)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || conditions != nil {
		t.Fatalf("conflicting source revisions = %#v, %v", conditions, err)
	}
}

// Rationale: a separately pinned Service root remains an independent source
// fence and must not disappear when the current Environment head is compared.
func TestManualScriptSourceConditionsPreserveIndependentServiceFence(t *testing.T) {
	sources := manualScriptSourceTestFixture()
	sources.Service.Record.desiredFenceKey = environmentBlueprintRootKey("env_manual_script", "pinned")
	conditions, err := manualScriptSourceConditions(sources)
	if err != nil || len(conditions) != 3 {
		t.Fatalf("independent source fences = %#v, %v", conditions, err)
	}
	if conditions[0] != serviceDesiredCondition(sources.Service) {
		t.Fatalf("Service fence = %#v", conditions[0])
	}
	if err := validateIdempotencyPlanKeys(conditions, nil); err != nil {
		t.Fatal(err)
	}
}

func manualScriptSourceTestFixture() ScriptExecutionSources {
	const environmentID = "env_manual_script"
	return ScriptExecutionSources{
		Environment: Versioned[EnvironmentRecord]{Record: EnvironmentRecord{ID: environmentID}},
		Service: Versioned[ServiceRecord]{
			Record: ServiceRecord{desiredFenceKey: environmentBlueprintHeadKey(environmentID)}, Revision: 13,
		},
		DesiredHead: Versioned[EnvironmentBlueprintHead]{Revision: 13},
		DesiredProjection: Versioned[EnvironmentComposeProjection]{
			Record: EnvironmentComposeProjection{RevisionID: "revision"}, Revision: 11,
		},
	}
}
