package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestReleasePlanningAppliedProjectionUsesCapturedRevisionNotDesiredHead(t *testing.T) {
	ctx := context.Background()
	store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 901)
	initial := releasePlanningTestProjection(environment.Record.ID, task)
	value, err := encodeEnvironmentComposeProjection(initial)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Put(ctx, environmentComposeProjectionKey(environment.Record.ID), value)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	scope := ReleasePlanningScope{
		Environment:  environment,
		ReadRevision: first,
		Compose:      Versioned[EnvironmentComposeProjection]{Record: initial},
	}
	// Intervening no-candidate Apply advances the acknowledged projection while
	// the caller's captured predecessor revision must stay fixed.
	nextTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 902)
	next := releasePlanningTestProjection(environment.Record.ID, nextTask)
	next.RevisionID = ids.New(ids.KindTask)
	nextValue, err := encodeEnvironmentComposeProjection(next)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(ctx, environmentComposeProjectionKey(environment.Record.ID), nextValue)
	if err != nil {
		t.Fatal(err)
	}
	prior, found, err := ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil || !found || prior.Revision != first || prior.Record.RevisionID != initial.RevisionID {
		t.Fatalf("captured predecessor = %+v, %t, %v", prior, found, err)
	}
	scope.ReadRevision = second
	applied, found, err := ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil || !found || applied.Revision != second || applied.Record.RevisionID != next.RevisionID ||
		applied.Record.RevisionID == scope.Compose.Record.RevisionID {
		t.Fatalf("applied vs desired = %+v, %t, %v", applied, found, err)
	}
}
