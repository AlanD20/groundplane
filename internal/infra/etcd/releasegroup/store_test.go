package releasegroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	infraetcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestStoreCreateValidatesMembershipScopeAndReplays(t *testing.T) {
	t.Parallel()

	backend := newMemoryStore()
	store, err := New(backend)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	group, environmentID, serviceIDs := fixtureGroup(t, 1, "realtime")
	seedEnvironmentAndServices(backend, environmentID, serviceIDs)

	created, err := store.createDirectFixture(context.Background(), group)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	replayed, err := store.createDirectFixture(context.Background(), group)
	if err != nil {
		t.Fatalf("replayed Create() error = %v", err)
	}
	if replayed.Revision != created.Revision || !domain.Equal(replayed.Group, created.Group) {
		t.Fatalf("replayed Create() = %+v, want %+v", replayed, created)
	}

	conflict, _, _ := fixtureGroup(t, 20, "realtime")
	conflict.EnvironmentID = environmentID
	conflict.ServiceIDs = append([]string(nil), serviceIDs...)
	conflict.Order = append([]string(nil), serviceIDs...)
	_, err = store.createDirectFixture(context.Background(), conflict)
	if !errors.Is(err, errs.New(errs.KindNameConflict, "")) {
		t.Fatalf("conflicting Create() error = %v, want name.conflict", err)
	}

	foreign, foreignEnvironmentID, foreignServices := fixtureGroup(t, 40, "foreign")
	seedEnvironmentAndServices(backend, foreignEnvironmentID, foreignServices)
	projectionResult, err := backend.Get(context.Background(), environmentComposeKey(foreignEnvironmentID))
	if err != nil || projectionResult.Entry == nil {
		t.Fatalf("read foreign desired projection: %#v, %v", projectionResult, err)
	}
	projection, err := decodeDurable[infraetcd.EnvironmentComposeProjection](
		projectionResult.Entry.Value, "environment-compose-projection",
	)
	if err != nil {
		t.Fatalf("decode foreign desired projection: %v", err)
	}
	projection.DesiredServices = projection.DesiredServices[:1]
	backend.put(environmentComposeKey(foreignEnvironmentID), mustDurableValue(
		"environment-compose-projection", projection,
	))
	_, err = store.createDirectFixture(context.Background(), foreign)
	if !errors.Is(err, errs.New(errs.KindServiceNotFound, "")) {
		t.Fatalf("cross-scope Create() error = %v, want service.not_found", err)
	}
}

func TestStoreUpdateUsesRevisionAndIsReplaySafe(t *testing.T) {
	t.Parallel()

	backend := newMemoryStore()
	store, _ := New(backend)
	group, environmentID, serviceIDs := fixtureGroup(t, 60, "realtime")
	seedEnvironmentAndServices(backend, environmentID, serviceIDs)
	created, err := store.createDirectFixture(context.Background(), group)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	replacement, err := domain.Replace(group, domain.Desired{
		Name: "workers", ServiceIDs: serviceIDs, Order: []string{serviceIDs[1], serviceIDs[0]},
		OnFailure: domain.OnFailureLeaveActive,
	})
	if err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	updated, err := store.updateDirectFixture(context.Background(), created, replacement)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	replayed, err := store.updateDirectFixture(context.Background(), created, replacement)
	if err != nil {
		t.Fatalf("replayed Update() error = %v", err)
	}
	if replayed.Revision != updated.Revision || !domain.Equal(replayed.Group, updated.Group) {
		t.Fatalf("replayed Update() = %+v, want %+v", replayed, updated)
	}

	stale := created
	stale.Revision++
	stale.ReadRevision++
	_, err = store.updateDirectFixture(context.Background(), stale, group)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("stale Update() error = %v, want state.conflict", err)
	}
}

func TestStoreRemoveCannotBypassTaskFinalization(t *testing.T) {
	// Rationale: Release Group deletion must retain the primary and indexes
	// until a durable Controller Task completes; raw CRUD removal is forbidden.
	t.Parallel()

	backend := newMemoryStore()
	store, _ := New(backend)
	group, environmentID, serviceIDs := fixtureGroup(t, 80, "realtime")
	seedEnvironmentAndServices(backend, environmentID, serviceIDs)
	created, err := store.createDirectFixture(context.Background(), group)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if _, err = store.removeDirectForbidden(context.Background(), created); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Remove() error = %v, want protected-finalizer failure", err)
	}
	if retained, err := store.Get(context.Background(), group.ID); err != nil || retained.Group.ID != group.ID {
		t.Fatalf("Get() after rejected raw removal = %+v, %v", retained, err)
	}
	if result, err := backend.Get(context.Background(), environmentComposeKey(environmentID)); err != nil || result.Entry == nil {
		t.Fatalf("member Service desired projection was removed: %+v, %v", result, err)
	}
}

func TestStoreListUsesEnvironmentScopeAndCursorRevision(t *testing.T) {
	// Rationale: cursors are fixed-revision capabilities, not caller-authored
	// seek offsets; scope, limit, last-id identity, and anchor must all match.
	t.Parallel()

	backend := newMemoryStore()
	store, _ := New(backend)
	first, environmentID, serviceIDs := fixtureGroup(t, 100, "alpha")
	second, _, _ := fixtureGroup(t, 110, "beta")
	second.EnvironmentID = environmentID
	second.ServiceIDs = append([]string(nil), serviceIDs...)
	second.Order = append([]string(nil), serviceIDs...)
	seedEnvironmentAndServices(backend, environmentID, serviceIDs)
	if _, err := store.createDirectFixture(context.Background(), first); err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}
	if _, err := store.createDirectFixture(context.Background(), second); err != nil {
		t.Fatalf("Create(second) error = %v", err)
	}

	page, err := store.List(context.Background(), environmentID, PageRequest{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("List(first page) = %+v, %v", page, err)
	}
	next, err := store.List(context.Background(), environmentID, PageRequest{Limit: 1, Cursor: page.NextCursor})
	if err != nil || len(next.Items) != 1 || next.Revision != page.Revision || next.Items[0].Group.ID == page.Items[0].Group.ID {
		t.Fatalf("List(next page) = %+v, %v", next, err)
	}
}

func TestStoreRejectsTamperedCursorAndCorruptIndexes(t *testing.T) {
	// Rationale: accepting a validly encoded but invented last id can silently
	// skip records, while a mismatched secondary index can return the wrong owner.
	t.Parallel()

	backend := newMemoryStore()
	store, _ := New(backend)
	group, environmentID, serviceIDs := fixtureGroup(t, 130, "alpha")
	seedEnvironmentAndServices(backend, environmentID, serviceIDs)
	created, err := store.createDirectFixture(context.Background(), group)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	forged, err := encodeCursor(cursor{
		Version: 1, Revision: created.ReadRevision, EnvironmentID: environmentID,
		LastID: ids.NewAt(ids.KindReleaseGroup, time.Date(2026, 8, 26, 11, 0, 0, 0, time.UTC), 999), Limit: 1,
	})
	if err != nil {
		t.Fatalf("encodeCursor() error = %v", err)
	}
	if _, err := store.List(context.Background(), environmentID, PageRequest{Limit: 1, Cursor: forged}); !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
		t.Fatalf("List(forged cursor) error = %v, want 400 malformed request", err)
	}

	backend.put(ownerKey(environmentID, group.ID), []byte(ids.NewAt(ids.KindReleaseGroup, time.Now(), 1)))
	if _, err := store.Get(context.Background(), group.ID); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Get(corrupt owner index) error = %v, want internal corruption", err)
	}
}

func TestStoreMutationAdvancesEnvironmentEpoch(t *testing.T) {
	// Rationale: every desired-state write must invalidate plans sealed against
	// the previous Environment coordination revision in the same transaction.
	t.Parallel()

	backend := newMemoryStore()
	store, _ := New(backend)
	group, environmentID, serviceIDs := fixtureGroup(t, 150, "alpha")
	seedEnvironmentAndServices(backend, environmentID, serviceIDs)
	before, _ := backend.Get(context.Background(), environmentMutationEpochKey(environmentID))
	if _, err := store.createDirectFixture(context.Background(), group); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	after, _ := backend.Get(context.Background(), environmentMutationEpochKey(environmentID))
	if before.Entry == nil || after.Entry == nil || after.Entry.ModRevision <= before.Entry.ModRevision {
		t.Fatalf("mutation epoch revisions before=%+v after=%+v", before.Entry, after.Entry)
	}
}

func fixtureGroup(t *testing.T, seed int64, name string) (domain.Group, string, []string) {
	t.Helper()
	now := time.Date(2026, 8, 26, 11, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, seed+1)
	serviceIDs := []string{
		ids.NewAt(ids.KindService, now, seed+2), ids.NewAt(ids.KindService, now, seed+3),
	}
	group, err := domain.New(domain.Input{
		ID: ids.NewAt(ids.KindReleaseGroup, now, seed), EnvironmentID: environmentID,
		Name: name, ServiceIDs: serviceIDs,
	})
	if err != nil {
		t.Fatalf("domain.New() error = %v", err)
	}
	return group, environmentID, serviceIDs
}

func seedEnvironmentAndServices(store *memoryStore, environmentID string, serviceIDs []string) {
	now := time.Date(2026, 8, 26, 11, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 8001)
	projectID := ids.NewAt(ids.KindProject, now, 8002)
	createTaskID := ids.NewAt(ids.KindTask, now, 8003)
	blueprintRevisionID := ids.NewAt(ids.KindTask, now, 8004)
	tenant := infraetcd.TenantRecord{ID: tenantID, Slug: "tenant", Name: "Tenant"}
	project := infraetcd.ProjectRecord{ID: projectID, TenantID: tenantID, Slug: "project", Name: "Project", Kind: infraetcd.ProjectKindTenant}
	environment := infraetcd.EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.0.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: infraetcd.EnvironmentProvisioningReady, CreateTaskID: createTaskID, CreatedAt: now,
	}
	store.put(tenantKey(tenantID), mustDurableValue("tenant", tenant))
	store.put(projectKey(projectID), mustDurableValue("project", project))
	store.put(projectOwnerKey(project), []byte(projectID))
	store.put(environmentKey(environmentID), mustDurableValue("environment", environment))
	store.put(environmentOwnerKey(projectID, environmentID), []byte(environmentID))
	store.put(environmentMutationEpochKey(environmentID), mustDurableValue("environment-mutation-epoch", epochRecord{EnvironmentID: environmentID}))
	projection := infraetcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: blueprintRevisionID, RenderGeneration: 1,
		DesiredServices: make([]infraetcd.EnvironmentServiceProjection, len(serviceIDs)),
	}
	for index, serviceID := range serviceIDs {
		name := fmt.Sprintf("service-%02d", index)
		desired := core.Service{ID: serviceID, Name: name, Image: "example.invalid/image:tag"}
		projection.DesiredServices[index] = infraetcd.EnvironmentServiceProjection{
			EnvironmentID: environmentID,
			Desired:       desired,
		}
		store.put(serviceKey(serviceID), mustDurableValue("service", infraetcd.ServiceRecord{
			EnvironmentID: environmentID, Desired: desired,
			Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
		}))
		store.put(serviceOwnerKey(environmentID, serviceID), []byte(serviceID))
	}
	store.put(environmentComposeKey(environmentID), mustDurableValue("environment-compose-projection", projection))
}

func mustDurableValue[T any](kind string, value T) []byte {
	encoded, err := json.Marshal(durableEnvelope[T]{Schema: 1, Kind: kind, Data: value})
	if err != nil {
		panic(err)
	}
	return encoded
}

type memoryStore struct {
	mu       sync.Mutex
	revision int64
	values   map[string]infraetcd.KeyValue
}

func newMemoryStore() *memoryStore {
	return &memoryStore{revision: 1, values: make(map[string]infraetcd.KeyValue)}
}

func (store *memoryStore) Health(context.Context) error              { return nil }
func (store *memoryStore) Close() error                              { return nil }
func (store *memoryStore) Snapshot(context.Context, io.Writer) error { return nil }
func (store *memoryStore) Watch(context.Context, string, int64) (*infraetcd.WatchStream, error) {
	return nil, errors.New("not implemented")
}

func (store *memoryStore) Get(_ context.Context, key string) (*infraetcd.GetResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value := store.value(key)
	return &infraetcd.GetResult{Entry: value, ReadRevision: store.revision}, nil
}

func (store *memoryStore) GetMany(_ context.Context, request infraetcd.GetManyRequest) (*infraetcd.GetManyResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	values := make([]*infraetcd.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.value(key)
	}
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	return &infraetcd.GetManyResult{Values: values, ReadRevision: revision, ResponseRevision: store.revision}, nil
}

func (store *memoryStore) Put(_ context.Context, key string, value []byte) (int64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.putLocked(key, value)
	return store.revision, nil
}

func (store *memoryStore) Delete(_ context.Context, key string) (int64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.revision++
	delete(store.values, key)
	return store.revision, nil
}

func (store *memoryStore) Range(_ context.Context, request infraetcd.RangeRequest) (*infraetcd.RangeResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0)
	for key := range store.values {
		if strings.HasPrefix(key, request.Prefix) && key > request.StartExclusive {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	more := int64(len(keys)) > request.Limit
	if request.Limit > 0 && int64(len(keys)) > request.Limit {
		keys = keys[:request.Limit]
	}
	values := make([]infraetcd.KeyValue, len(keys))
	for index, key := range keys {
		values[index] = *store.value(key)
	}
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	return &infraetcd.RangeResult{Values: values, ReadRevision: revision, ResponseRevision: store.revision, More: more}, nil
}

func (store *memoryStore) Transact(_ context.Context, conditions []infraetcd.Condition, mutations []infraetcd.Mutation) (infraetcd.TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	failures := make([]*infraetcd.KeyValue, len(conditions))
	succeeded := true
	for index, condition := range conditions {
		value := store.value(condition.Key)
		failures[index] = value
		if condition.Prefix {
			for key := range store.values {
				if strings.HasPrefix(key, condition.Key) {
					succeeded = false
				}
			}
			continue
		}
		if condition.ModRevision == 0 {
			succeeded = succeeded && value == nil
		} else {
			succeeded = succeeded && value != nil && value.ModRevision == condition.ModRevision
		}
	}
	if !succeeded {
		return infraetcd.TransactionResult{Revision: store.revision, FailureReads: failures}, nil
	}
	store.revision++
	for _, mutation := range mutations {
		if mutation.Type == infraetcd.MutationDelete {
			delete(store.values, mutation.Key)
		} else {
			store.values[mutation.Key] = infraetcd.KeyValue{Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), Version: 1, ModRevision: store.revision}
		}
	}
	return infraetcd.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryStore) put(key string, value []byte) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.putLocked(key, value)
}

func (store *memoryStore) putLocked(key string, value []byte) {
	store.revision++
	store.values[key] = infraetcd.KeyValue{Key: key, Value: append([]byte(nil), value...), Version: 1, ModRevision: store.revision}
}

func (store *memoryStore) delete(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.revision++
	delete(store.values, key)
}

func (store *memoryStore) value(key string) *infraetcd.KeyValue {
	value, exists := store.values[key]
	if !exists {
		return nil
	}
	value.Value = append([]byte(nil), value.Value...)
	return &value
}
