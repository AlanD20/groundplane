package etcd

import (
	"context"
	"errors"
	"io"
	"testing"
)

type releasePlanningTestStore struct {
	*memoryHierarchyStore
}

func (*releasePlanningTestStore) Health(context.Context) error { return nil }

func (store *releasePlanningTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: key, Value: value}})
	return result.Revision, err
}

func (store *releasePlanningTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: key}})
	return result.Revision, err
}

func (*releasePlanningTestStore) Watch(context.Context, string, int64) (*WatchStream, error) {
	return nil, errors.New("release planning test store does not support watches")
}

func (*releasePlanningTestStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("release planning test store does not support snapshots")
}

func (*releasePlanningTestStore) Close() error { return nil }

func releasePlanningTestProjection(
	environmentID string,
	task TaskRecord,
) EnvironmentComposeProjection {
	projection := environmentBlueprintTestProjection(environmentID, task, 1)
	service := projection.DesiredServices[0].Desired
	projection.DesiredServices = []EnvironmentServiceProjection{{
		EnvironmentID: environmentID,
		Desired:       service,
	}}
	return projection
}

func TestReleasePlanningLoadsPublishedBlueprintProjection(t *testing.T) {
	ctx := context.Background()
	store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 90)
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	projection := releasePlanningTestProjection(environment.Record.ID, task)
	result := publishEnvironmentBlueprintTestRevision(
		t,
		hierarchy,
		project,
		environment,
		0,
		revision,
		projection,
		environmentBlueprintTestZoneChanges(t, hierarchy, projection),
		environmentBlueprintTestServiceChanges(t, hierarchy, projection),
		environmentBlueprintTestRouteChanges(t, hierarchy, projection),
		ComponentTaskPreparation{},
		task,
		environmentBlueprintTestMarker(task, environment.Record.ID),
	)
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("publication outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	tasks, err := NewTaskRepository(store)
	if err != nil {
		t.Fatalf("NewTaskRepository() error = %v", err)
	}
	ledger, err := NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatalf("NewReleaseLedger() error = %v", err)
	}
	scope, err := ledger.LoadPlanningScope(ctx, environment.Record.ID)
	if err != nil {
		t.Fatalf("LoadPlanningScope() error = %v", err)
	}
	if scope.Compose.Record.RevisionID != task.ID || scope.Compose.ReadRevision != scope.ReadRevision {
		t.Fatalf("planning Compose projection = %#v", scope.Compose)
	}
	if _, err := store.Put(ctx, "/unrelated/global-write", []byte("unrelated")); err != nil {
		t.Fatalf("unrelated write: %v", err)
	}
	fixed, err := ledger.LoadPlanningScopeAtRevision(ctx, environment.Record.ID, scope.ReadRevision)
	if err != nil || fixed.ReadRevision != scope.ReadRevision ||
		fixed.EnvironmentEpochRevision != scope.EnvironmentEpochRevision {
		t.Fatalf("LoadPlanningScopeAtRevision() = %#v, %v", fixed, err)
	}
	planning, err := ledger.LoadPlanningServices(ctx, scope, []string{projection.DesiredServices[0].Desired.ID})
	if err != nil {
		t.Fatalf("LoadPlanningServices() error = %v", err)
	}
	if len(planning) != 1 || planning[0].Service.Record.Desired.ID != projection.DesiredServices[0].Desired.ID {
		t.Fatalf("planning Services = %#v", planning)
	}
}
