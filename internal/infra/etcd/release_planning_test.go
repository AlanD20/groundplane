package etcd

import (
	"context"
	"errors"
	"io"
	"testing"

	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type releasePlanningTestStore struct {
	*memoryHierarchyStore
}

func (*releasePlanningTestStore) Health(context.Context) error { return nil }

func (store *releasePlanningTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	return result.Revision, err
}

func (store *releasePlanningTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}})
	return result.Revision, err
}

func (*releasePlanningTestStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, errors.New("release planning test store does not support watches")
}

func (*releasePlanningTestStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("release planning test store does not support snapshots")
}

func (*releasePlanningTestStore) Close() error { return nil }

func releasePlanningTestProjection(
	t *testing.T,
	environmentID, revisionID string,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	return routeRepositoryTestProjection(t, environmentID, "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", revisionID)
}

func TestReleasePlanningLoadsPublishedBlueprintProjection(t *testing.T) {
	ctx := context.Background()
	_, base, environment, _, service := routeRepositoryTestHierarchy(t)
	store := &releasePlanningTestStore{memoryHierarchyStore: base}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	desired, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("desired projection = %#v, %v, %v", desired, found, err)
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
	if scope.Compose.Record.RevisionID != desired.Record.RevisionID ||
		scope.Compose.ReadRevision != scope.ReadRevision {
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
	planning, err := ledger.LoadPlanningServices(ctx, scope, []string{service.Record.Desired.ID})
	if err != nil {
		t.Fatalf("LoadPlanningServices() error = %v", err)
	}
	if len(planning) != 1 || planning[0].Service.Record.Desired.ID != service.Record.Desired.ID {
		t.Fatalf("planning Services = %#v", planning)
	}
}
