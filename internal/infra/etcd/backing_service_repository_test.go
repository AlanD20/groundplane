package etcd

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingReadRaceStore struct {
	*memoryHierarchyStore
	deleteKey string
	armed     bool
	injected  bool
}

func (store *backingReadRaceStore) Range(ctx context.Context, request RangeRequest) (*RangeResult, error) {
	result, err := store.memoryHierarchyStore.Range(ctx, request)
	if err != nil || !store.armed || store.injected || request.Prefix != projectPlatformOwnerPrefix {
		return result, err
	}
	store.injected = true
	_, err = store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: store.deleteKey}})
	return result, err
}

// Rationale: a backing-service page must join its Project, main Environment, and adapter Service
// at the cursor's fixed MVCC revision even when a child changes during the request.
func TestBackingServiceRepositoryReadsFacadeFromOneSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := &backingReadRaceStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	want := seedBackingService(t, store, 900, "shared-postgres")
	store.deleteKey = serviceKey(want.ServiceID)
	store.armed = true
	repository, err := newBackingServiceRepository(store)
	if err != nil {
		t.Fatalf("newBackingServiceRepository() error = %v", err)
	}
	page, err := repository.ListBackingServices(ctx, PageRequest{Limit: 1})
	if err != nil || !store.injected || len(page.Items) != 1 || !reflect.DeepEqual(page.Items[0].Record, want) {
		t.Fatalf("ListBackingServices() = %#v, %v, injected %t", page, err, store.injected)
	}
	if page.Items[0].ReadRevision != page.Revision || page.Revision >= store.revision {
		t.Fatalf(
			"snapshot revisions = item %d, page %d, latest %d",
			page.Items[0].ReadRevision,
			page.Revision,
			store.revision,
		)
	}
}

// Rationale: the facade is valid only for a platform-owned backing Project with exactly one
// main Environment and one adapter Service; a tenant Project must not be exposed under this noun.
func TestBackingServiceRepositoryRejectsNonBackingProject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 930), Slug: "acme", Name: "Acme"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 931), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	if _, err := hierarchy.CreateProject(ctx, project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	repository, err := newBackingServiceRepository(store)
	if err != nil {
		t.Fatalf("newBackingServiceRepository() error = %v", err)
	}
	if _, err := repository.GetBackingService(ctx, project.ID); !isKind(err, errs.KindBackingServiceNotFound) {
		t.Fatalf("GetBackingService() error = %v", err)
	}
}

func seedBackingService(
	t *testing.T,
	store hierarchyStore,
	offset int64,
	slug string,
) BackingServiceRecord {
	t.Helper()
	ctx := context.Background()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	services, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	projectRecord := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, offset), Slug: slug,
		Name: "Shared PostgreSQL", Kind: ProjectKindBacking,
	}
	project, err := hierarchy.CreateProject(ctx, projectRecord)
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := hierarchyTestID(ids.KindEnvironment, offset+1)
	environmentRecord, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		projectRecord,
		environmentID,
		"main",
		"10.31.0.0/16",
		hierarchyTestID(ids.KindTask, offset+2),
		time.Unix(offset, 0).UTC(),
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	environmentValue, err := encodeEnvironment(environmentRecord)
	if err != nil {
		t.Fatalf("encodeEnvironment() error = %v", err)
	}
	result, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: environmentKey(environmentID), Value: environmentValue},
		{Type: MutationPut, Key: environmentNameKey(projectRecord.ID, "main"), Value: []byte(environmentID)},
		{Type: MutationPut, Key: environmentOwnerKey(projectRecord.ID, environmentID), Value: []byte(environmentID)},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed backing Environment = %#v, %v", result, err)
	}
	environment, err := hierarchy.GetEnvironment(ctx, environmentID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	serviceID := hierarchyTestID(ids.KindService, offset+3)
	serviceRecord, err := NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "postgres", Image: "postgres:16-alpine",
		Strategy: core.StrategyRecreate, Adapter: "postgres:16", FactsPrefix: "pg16_",
	}, hierarchyTestID(ids.KindNetwork, offset+4))
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	if _, err := services.CreateService(ctx, environment, project, serviceRecord); err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	return BackingServiceRecord{ProjectID: projectRecord.ID, EnvironmentID: environmentID, ServiceID: serviceID}
}
