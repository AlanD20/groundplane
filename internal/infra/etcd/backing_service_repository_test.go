package etcd

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackingservices "github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingReadRaceStore struct {
	*memoryHierarchyStore
	deleteKey string
	armed     bool
	injected  bool
}

func (store *backingReadRaceStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	result, err := store.memoryHierarchyStore.Range(ctx, request)
	if err != nil || !store.armed || store.injected || request.Prefix != testhierarchy.ProjectPlatformOwnerPrefix {
		return result, err
	}
	store.injected = true
	_, err = store.memoryHierarchyStore.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: store.deleteKey}},
	)
	return result, err
}

// Rationale: a backing-service page must join its Project, main Environment, and adapter Service
// at the cursor's fixed MVCC revision even when a child changes during the request.
func TestBackingServiceRepositoryReadsFacadeFromOneSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := &backingReadRaceStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	want := seedBackingService(t, store, 900, "shared-postgres")
	store.deleteKey = testservices.ServiceRuntimeKey(want.ServiceID)
	store.armed = true
	repository, err := testbackingservices.NewRepository(store)
	if err != nil {
		t.Fatalf("newBackingServiceRepository() error = %v", err)
	}
	page, err := repository.ListBackingServices(ctx, testkeyvalue.PageRequest{Limit: 1})
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
	services, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	current, err := services.GetService(ctx, want.ServiceID)
	if err != nil || current.Record.Desired.Name != "postgres" || current.Record.Desired.Image != "postgres:16-alpine" {
		t.Fatalf("GetService(current head) = %#v, %v", current, err)
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
	tenant := testhierarchy.TenantRecord{ID: hierarchyTestID(ids.KindTenant, 930), Slug: "acme", Name: "Acme"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 931), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: testhierarchy.ProjectKindTenant,
	}
	if _, err := hierarchy.CreateProject(ctx, project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	repository, err := testbackingservices.NewRepository(store)
	if err != nil {
		t.Fatalf("newBackingServiceRepository() error = %v", err)
	}
	if _, err := repository.GetBackingService(ctx, project.ID); !isKind(err, errs.KindBackingServiceNotFound) {
		t.Fatalf("GetBackingService() error = %v", err)
	}
}

// Rationale: the backing facade must continue to resolve stable identity and
// desired state when its independently mutable runtime sidecar is absent.
func TestBackingServiceRepositoryDefaultsMissingRuntimeToRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	want := seedBackingService(t, store, 940, "missing-runtime")
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationDelete, Key: testservices.ServiceRuntimeKey(want.ServiceID),
	}}); err != nil {
		t.Fatalf("Delete(Service runtime sidecar) error = %v", err)
	}
	repository, err := testbackingservices.NewRepository(store)
	if err != nil {
		t.Fatalf("newBackingServiceRepository() error = %v", err)
	}
	resolved, err := repository.GetBackingService(ctx, want.ProjectID)
	if err != nil || resolved.Record != want {
		t.Fatalf("GetBackingService(missing runtime) = %#v, %v", resolved, err)
	}
	services, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	service, err := services.GetService(ctx, want.ServiceID)
	if err != nil || service.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("GetService(missing runtime) = %#v, %v", service, err)
	}
}

func seedBackingService(
	t *testing.T,
	store hierarchyStore,
	offset int64,
	slug string,
) testbackingservices.Record {
	t.Helper()
	ctx := context.Background()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	projectRecord := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, offset), Slug: slug,
		Name: "Shared PostgreSQL", Kind: testhierarchy.ProjectKindBacking,
	}
	if _, err := hierarchy.CreateProject(ctx, projectRecord); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := hierarchyTestID(ids.KindEnvironment, offset+1)
	environmentRecord, err := testhierarchy.NewProvisioningEnvironment(
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
	environmentValue, err := testhierarchy.EncodeEnvironment(environmentRecord)
	if err != nil {
		t.Fatalf("encodeEnvironment() error = %v", err)
	}
	epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: environmentID,
		},
	)
	if err != nil {
		t.Fatalf("encodeEnvironmentMutationEpochRecord() error = %v", err)
	}
	scriptSetValue, err := testscripts.EncodeScriptSetGeneration(testscripts.SetGenerationRecord{
		EnvironmentID: environmentID, GenerationID: environmentID,
	})
	if err != nil {
		t.Fatalf("encodeScriptSetGeneration() error = %v", err)
	}
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentKey(environmentID), Value: environmentValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentNameKey(projectRecord.ID, "main"),
			Value: []byte(environmentID),
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentOwnerKey(projectRecord.ID, environmentID),
			Value: []byte(environmentID),
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentMutationEpochKey(environmentID),
			Value: epochValue,
		},
		{Type: testkeyvalue.MutationPut, Key: testscripts.ScriptSetActiveKey(environmentID), Value: scriptSetValue},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed backing Environment = %#v, %v", result, err)
	}
	serviceID := hierarchyTestID(ids.KindService, offset+3)
	backingNetworkID := hierarchyTestID(ids.KindNetwork, offset+4)
	old := seedDesiredServiceFixture(t, ctx, store, environmentID, core.Service{
		ID: serviceID, Name: "postgres-old", Image: "postgres:15-alpine",
		Strategy: core.StrategyRecreate, Adapter: "postgres:16", FactsPrefix: "pg16_",
	}, backingNetworkID, offset+10, true, true)
	current := seedDesiredServiceFixture(t, ctx, store, environmentID, core.Service{
		ID: serviceID, Name: "postgres", Image: "postgres:16-alpine",
		Strategy: core.StrategyRecreate, Adapter: "postgres:16", FactsPrefix: "pg16_",
	}, backingNetworkID, offset+11, true, true)
	if old.Service.Record.Desired.ID != current.Service.Record.Desired.ID ||
		old.Projection.Revision >= current.Projection.Revision {
		t.Fatalf("desired head fixture revisions = %d/%d", old.Projection.Revision, current.Projection.Revision)
	}
	return testbackingservices.Record{
		ProjectID: projectRecord.ID, EnvironmentID: environmentID, ServiceID: serviceID,
		BackingNetworkID: current.Service.Record.BackingNetworkID,
	}
}
