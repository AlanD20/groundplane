package etcd

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyCreateResolveAndRenamePreserveIdentity(t *testing.T) {
	// Rationale: labels are mutable while ids and owner indexes are stable;
	// create and rename must never expose two slug owners or move a primary.
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(): %v", err)
	}
	ctx := context.Background()
	tenantID := hierarchyTestID(ids.KindTenant, 1)
	projectID := hierarchyTestID(ids.KindProject, 2)
	environmentID := hierarchyTestID(ids.KindEnvironment, 3)

	tenant, err := repository.CreateTenant(ctx, TenantRecord{ID: tenantID, Slug: "acme", Name: "Acme"})
	if err != nil {
		t.Fatalf("CreateTenant(): %v", err)
	}
	project, err := repository.CreateProject(ctx, ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject(): %v", err)
	}
	environment, err := repository.CreateEnvironment(ctx, EnvironmentRecord{
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ID:                environmentID, ProjectID: projectID, Name: "production",
		VolumeDir: "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		CreatedAt: time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("CreateEnvironment(): %v", err)
	}

	renamed, err := repository.RenameTenant(ctx, tenantID, tenant.Revision, "acme-group")
	if err != nil {
		t.Fatalf("RenameTenant(): %v", err)
	}
	if renamed.Record.ID != tenantID || renamed.Record.Slug != "acme-group" || renamed.Record.Name != "Acme" {
		t.Fatalf("renamed tenant = %+v", renamed.Record)
	}
	if _, err := repository.ResolveTenant(ctx, "acme"); !errors.Is(err, errs.New(errs.KindTenantNotFound, "")) {
		t.Fatalf("ResolveTenant(old slug) error = %v, want tenant.not_found", err)
	}
	resolved, err := repository.ResolveTenant(ctx, "acme-group")
	if err != nil || resolved.Record.ID != tenantID {
		t.Fatalf("ResolveTenant(new slug) = %+v, %v", resolved, err)
	}
	afterTenantProject, err := repository.GetProject(ctx, projectID)
	if err != nil || afterTenantProject.Record != project.Record {
		t.Fatalf("project changed with tenant rename: %+v, %v", afterTenantProject.Record, err)
	}
	afterTenantEnvironment, err := repository.GetEnvironment(ctx, environmentID)
	if err != nil || afterTenantEnvironment.Record != environment.Record {
		t.Fatalf("environment changed with tenant rename: %+v, %v", afterTenantEnvironment.Record, err)
	}
	if _, err := repository.RenameTenantProject(
		ctx, projectID, project.Revision-1, "console-next",
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("RenameTenantProject(stale revision) error = %v, want state.conflict", err)
	}
	renamedProject, err := repository.RenameTenantProject(ctx, projectID, project.Revision, "console-next")
	if err != nil {
		t.Fatalf("RenameTenantProject(): %v", err)
	}
	if renamedProject.Record.ID != projectID || renamedProject.Record.TenantID != tenantID ||
		renamedProject.Record.Name != "Console" || renamedProject.Record.Kind != ProjectKindTenant {
		t.Fatalf("renamed project changed identity: %+v", renamedProject.Record)
	}
	if _, err := repository.ResolveTenantProject(
		ctx,
		tenantID,
		"console",
	); !errors.Is(
		err,
		errs.New(errs.KindProjectNotFound, ""),
	) {
		t.Fatalf("ResolveTenantProject(old slug) error = %v, want project.not_found", err)
	}
	afterProjectEnvironment, err := repository.GetEnvironment(ctx, environmentID)
	if err != nil || afterProjectEnvironment.Record != environment.Record {
		t.Fatalf("environment changed with project rename: %+v, %v", afterProjectEnvironment.Record, err)
	}
	revision := renamed.Revision
	noOp, err := repository.RenameTenant(ctx, tenantID, revision, "acme-group")
	if err != nil || noOp.Revision != revision || noOp.Record != renamed.Record {
		t.Fatalf("RenameTenant(current slug) = %+v, %v", noOp, err)
	}
}

func TestHierarchyScopedSlugUniquenessIsAtomic(t *testing.T) {
	// Rationale: tenant slugs are global while project slugs are tenant-scoped;
	// the unique index must reject one scope without leaking across another.
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(): %v", err)
	}
	ctx := context.Background()
	tenantA := hierarchyTestID(ids.KindTenant, 10)
	tenantB := hierarchyTestID(ids.KindTenant, 11)
	for _, tenant := range []TenantRecord{
		{ID: tenantA, Slug: "a", Name: "A"},
		{ID: tenantB, Slug: "b", Name: "B"},
	} {
		if _, err := repository.CreateTenant(ctx, tenant); err != nil {
			t.Fatalf("CreateTenant(%s): %v", tenant.ID, err)
		}
	}
	tenantBCurrent, err := repository.GetTenant(ctx, tenantB)
	if err != nil {
		t.Fatalf("GetTenant(B): %v", err)
	}
	if _, err := repository.RenameTenant(
		ctx,
		tenantB,
		tenantBCurrent.Revision,
		"a",
	); !errors.Is(
		err,
		errs.New(errs.KindSlugConflict, ""),
	) {
		t.Fatalf("RenameTenant(collision) error = %v, want slug.conflict", err)
	}
	if current, err := repository.ResolveTenant(ctx, "b"); err != nil || current.Record.ID != tenantB {
		t.Fatalf("ResolveTenant(original after collision) = %+v, %v", current, err)
	}
	if _, err := repository.CreateTenant(ctx, TenantRecord{
		ID: hierarchyTestID(ids.KindTenant, 12), Slug: "a", Name: "Duplicate",
	}); !errors.Is(err, errs.New(errs.KindSlugConflict, "")) {
		t.Fatalf("CreateTenant(duplicate slug) error = %v, want slug.conflict", err)
	}
	for index, tenantID := range []string{tenantA, tenantB} {
		_, err := repository.CreateProject(ctx, ProjectRecord{
			ID: hierarchyTestID(ids.KindProject, int64(20+index)), TenantID: tenantID,
			Slug: "shared-name", Name: "Shared Name", Kind: ProjectKindTenant,
		})
		if err != nil {
			t.Fatalf("CreateProject(scope %s): %v", tenantID, err)
		}
	}
	if _, err := repository.CreateProject(ctx, ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 22), TenantID: tenantA,
		Slug: "shared-name", Name: "Duplicate", Kind: ProjectKindTenant,
	}); !errors.Is(err, errs.New(errs.KindSlugConflict, "")) {
		t.Fatalf("CreateProject(duplicate scoped slug) error = %v, want slug.conflict", err)
	}
}

func TestHierarchyOwnerMustExistAtCreateCommit(t *testing.T) {
	// Rationale: an owner lookup followed by a child write is racy unless the
	// child transaction compares the exact parent modification revision.
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(): %v", err)
	}
	_, err = repository.CreateProject(context.Background(), ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 31), TenantID: hierarchyTestID(ids.KindTenant, 30),
		Slug: "orphan", Name: "Orphan", Kind: ProjectKindTenant,
	})
	if !errors.Is(err, errs.New(errs.KindTenantNotFound, "")) {
		t.Fatalf("CreateProject(orphan) error = %v, want tenant.not_found", err)
	}
}

func TestHierarchyPaginationPinsRevisionAndOrdersByID(t *testing.T) {
	// Rationale: inserting an id between pages must not duplicate, skip, or add
	// a record to a traversal whose cursor pins an earlier MVCC revision.
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(): %v", err)
	}
	ctx := context.Background()
	firstID := hierarchyTestID(ids.KindTenant, 40)
	secondID := hierarchyTestID(ids.KindTenant, 41)
	fourthID := hierarchyTestID(ids.KindTenant, 43)
	for _, record := range []TenantRecord{
		{ID: fourthID, Slug: "fourth", Name: "Fourth"},
		{ID: firstID, Slug: "first", Name: "First"},
		{ID: secondID, Slug: "second", Name: "Second"},
	} {
		if _, err := repository.CreateTenant(ctx, record); err != nil {
			t.Fatalf("CreateTenant(%s): %v", record.ID, err)
		}
	}
	pageOne, err := repository.ListTenants(ctx, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("ListTenants(first page): %v", err)
	}
	if got := tenantIDs(pageOne.Items); strings.Join(got, ",") != firstID+","+secondID {
		t.Fatalf("first page ids = %v", got)
	}
	if pageOne.NextCursor == "" {
		t.Fatal("first page cursor is empty")
	}
	thirdID := hierarchyTestID(ids.KindTenant, 42)
	if _, err := repository.CreateTenant(ctx, TenantRecord{ID: thirdID, Slug: "third", Name: "Third"}); err != nil {
		t.Fatalf("CreateTenant(third): %v", err)
	}
	pageTwo, err := repository.ListTenants(ctx, PageRequest{Limit: 2, Cursor: pageOne.NextCursor})
	if err != nil {
		t.Fatalf("ListTenants(second page): %v", err)
	}
	if got := tenantIDs(pageTwo.Items); len(got) != 1 || got[0] != fourthID {
		t.Fatalf("second page ids = %v, want [%s]", got, fourthID)
	}
	if pageTwo.Revision != pageOne.Revision {
		t.Fatalf("second page revision = %d, want %d", pageTwo.Revision, pageOne.Revision)
	}
	if _, err := repository.ListTenants(ctx, PageRequest{
		Limit: 1, Cursor: pageOne.NextCursor,
	}); !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
		t.Fatalf("ListTenants(query mismatch) error = %v, want validation.failed", err)
	}
}

func TestHierarchyIndexedPaginationReadsPrimariesAtPinnedRevision(t *testing.T) {
	// Rationale: an owner-index page and its referenced primaries must describe
	// one real MVCC view even when a later rename changes a primary between pages.
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(): %v", err)
	}
	ctx := context.Background()
	tenantID := hierarchyTestID(ids.KindTenant, 70)
	if _, err := repository.CreateTenant(ctx, TenantRecord{
		ID: tenantID, Slug: "indexed", Name: "Indexed",
	}); err != nil {
		t.Fatalf("CreateTenant(): %v", err)
	}
	firstProjectID := hierarchyTestID(ids.KindProject, 71)
	secondProjectID := hierarchyTestID(ids.KindProject, 72)
	for _, project := range []ProjectRecord{
		{
			ID: firstProjectID, TenantID: tenantID, Slug: "first", Name: "First",
			Kind: ProjectKindTenant,
		},
		{
			ID: secondProjectID, TenantID: tenantID, Slug: "second", Name: "Second",
			Kind: ProjectKindTenant,
		},
	} {
		if _, err := repository.CreateProject(ctx, project); err != nil {
			t.Fatalf("CreateProject(%s): %v", project.ID, err)
		}
	}
	pageOne, err := repository.ListTenantProjects(ctx, tenantID, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("ListTenantProjects(first page): %v", err)
	}
	second, err := repository.GetProject(ctx, secondProjectID)
	if err != nil {
		t.Fatalf("GetProject(second): %v", err)
	}
	if _, err := repository.RenameTenantProject(
		ctx, secondProjectID, second.Revision, "second-renamed",
	); err != nil {
		t.Fatalf("RenameTenantProject(second): %v", err)
	}
	pageTwo, err := repository.ListTenantProjects(ctx, tenantID, PageRequest{
		Limit: 1, Cursor: pageOne.NextCursor,
	})
	if err != nil {
		t.Fatalf("ListTenantProjects(second page): %v", err)
	}
	if len(pageTwo.Items) != 1 || pageTwo.Items[0].Record.Slug != "second" {
		t.Fatalf("pinned second page = %+v, want pre-rename primary", pageTwo.Items)
	}
	fresh, err := repository.ListTenantProjects(ctx, tenantID, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("ListTenantProjects(fresh): %v", err)
	}
	if len(fresh.Items) != 2 || fresh.Items[1].Record.Slug != "second-renamed" {
		t.Fatalf("fresh page = %+v, want renamed primary", fresh.Items)
	}
}

func TestHierarchyRenameCannotCommitAfterDeletionBegins(t *testing.T) {
	// Rationale: deletion owns the target as soon as its tombstone exists, including
	// when it starts after rename preparation but before the record/index transaction.
	for _, test := range []struct {
		name   string
		inject bool
	}{
		{name: "already deleting"},
		{name: "deletion starts before commit", inject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newMemoryHierarchyStore()
			id := hierarchyTestID(ids.KindTenant, 90)
			tombstone := deletionTombstoneKey("tenant", id)
			store := hierarchyStore(base)
			if test.inject {
				store = &deletionRaceHierarchyStore{memoryHierarchyStore: base, tombstoneKey: tombstone}
			}
			repository, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatalf("newHierarchyRepository(): %v", err)
			}
			created, err := repository.CreateTenant(context.Background(), TenantRecord{
				ID: id, Slug: "before", Name: "Display Name",
			})
			if err != nil {
				t.Fatalf("CreateTenant(): %v", err)
			}
			if !test.inject {
				seedHierarchyTest(t, base, []Mutation{{
					Type: MutationPut, Key: tombstone, Value: []byte(`{"phase":"requested"}`),
				}})
			}
			_, err = repository.RenameTenant(context.Background(), id, created.Revision, "after")
			if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
				t.Fatalf("RenameTenant() error = %v, want resource.in_use", err)
			}
			current, getErr := repository.GetTenant(context.Background(), id)
			if getErr != nil || current.Record.Slug != "before" || current.Record.Name != "Display Name" {
				t.Fatalf("tenant after blocked rename = %+v, %v", current.Record, getErr)
			}
			if _, resolveErr := repository.ResolveTenant(
				context.Background(),
				"after",
			); !errors.Is(
				resolveErr,
				errs.New(errs.KindTenantNotFound, ""),
			) {
				t.Fatalf("ResolveTenant(after) error = %v, want tenant.not_found", resolveErr)
			}
		})
	}
}

func TestHierarchyRejectsCorruptDurableEnvelope(t *testing.T) {
	// Rationale: unknown and duplicate durable fields are corruption, never
	// silently defaulted data or an operator validation failure.
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(): %v", err)
	}
	id := hierarchyTestID(ids.KindTenant, 50)
	_, err = store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut,
		Key:  tenantKey(id),
		Value: []byte(`{"schema":1,"kind":"tenant","data":{"id":"` + id +
			`","slug":"broken","slug":"duplicate","name":"Broken"}}`),
	}})
	if err != nil {
		t.Fatalf("seed corrupt record: %v", err)
	}
	if _, err := repository.GetTenant(context.Background(), id); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("GetTenant(corrupt) error = %v, want internal", err)
	}
}

func TestHierarchyRejectsRecordIDMismatchOnEveryReadPath(t *testing.T) {
	// Rationale: a schema-valid record under another stable id is durable
	// corruption; exact, slug, primary-list, and owner-index reads must all fail
	// closed rather than return an entity with the wrong identity.
	t.Run("direct get", func(t *testing.T) {
		store := newMemoryHierarchyStore()
		repository, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatalf("newHierarchyRepository(): %v", err)
		}
		keyID := hierarchyTestID(ids.KindTenant, 80)
		valueID := hierarchyTestID(ids.KindTenant, 81)
		value, err := encodeTenant(TenantRecord{ID: valueID, Slug: "wrong", Name: "Wrong"})
		if err != nil {
			t.Fatalf("encodeTenant(): %v", err)
		}
		seedHierarchyTest(t, store, []Mutation{{Type: MutationPut, Key: tenantKey(keyID), Value: value}})
		_, err = repository.GetTenant(context.Background(), keyID)
		assertInternalHierarchyError(t, err)
	})

	t.Run("slug resolve", func(t *testing.T) {
		store := newMemoryHierarchyStore()
		repository, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatalf("newHierarchyRepository(): %v", err)
		}
		keyID := hierarchyTestID(ids.KindTenant, 82)
		valueID := hierarchyTestID(ids.KindTenant, 83)
		value, err := encodeTenant(TenantRecord{ID: valueID, Slug: "indexed", Name: "Indexed"})
		if err != nil {
			t.Fatalf("encodeTenant(): %v", err)
		}
		seedHierarchyTest(t, store, []Mutation{
			{Type: MutationPut, Key: tenantKey(keyID), Value: value},
			{Type: MutationPut, Key: tenantSlugKey("indexed"), Value: []byte(keyID)},
		})
		_, err = repository.ResolveTenant(context.Background(), "indexed")
		assertInternalHierarchyError(t, err)
	})

	t.Run("primary list", func(t *testing.T) {
		store := newMemoryHierarchyStore()
		repository, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatalf("newHierarchyRepository(): %v", err)
		}
		keyID := hierarchyTestID(ids.KindTenant, 84)
		valueID := hierarchyTestID(ids.KindTenant, 85)
		value, err := encodeTenant(TenantRecord{ID: valueID, Slug: "listed", Name: "Listed"})
		if err != nil {
			t.Fatalf("encodeTenant(): %v", err)
		}
		seedHierarchyTest(t, store, []Mutation{{Type: MutationPut, Key: tenantKey(keyID), Value: value}})
		_, err = repository.ListTenants(context.Background(), PageRequest{})
		assertInternalHierarchyError(t, err)
	})

	t.Run("owner index", func(t *testing.T) {
		store := newMemoryHierarchyStore()
		repository, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatalf("newHierarchyRepository(): %v", err)
		}
		tenantID := hierarchyTestID(ids.KindTenant, 86)
		keyID := hierarchyTestID(ids.KindProject, 87)
		valueID := hierarchyTestID(ids.KindProject, 88)
		record := ProjectRecord{
			ID: valueID, TenantID: tenantID, Slug: "owned", Name: "Owned", Kind: ProjectKindTenant,
		}
		value, err := encodeProject(record)
		if err != nil {
			t.Fatalf("encodeProject(): %v", err)
		}
		seedHierarchyTest(t, store, []Mutation{
			{Type: MutationPut, Key: projectKey(keyID), Value: value},
			{Type: MutationPut, Key: projectTenantOwnerPrefix(tenantID) + keyID, Value: []byte(keyID)},
		})
		_, err = repository.ListTenantProjects(context.Background(), tenantID, PageRequest{})
		assertInternalHierarchyError(t, err)
	})
}

func TestHierarchyRejectsStandaloneBackingEnvironmentCreation(t *testing.T) {
	// Rationale: a backing project owns exactly one main environment created by
	// the atomic backing-service facade, never by generic environment CRUD.
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository(): %v", err)
	}
	ctx := context.Background()
	projectID := hierarchyTestID(ids.KindProject, 60)
	if _, err := repository.CreateProject(ctx, ProjectRecord{
		ID: projectID, Slug: "postgres", Name: "Postgres", Kind: ProjectKindBacking,
	}); err != nil {
		t.Fatalf("CreateProject(backing): %v", err)
	}
	_, err = repository.CreateEnvironment(ctx, EnvironmentRecord{
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ID:                hierarchyTestID(ids.KindEnvironment, 61), ProjectID: projectID, Name: "main",
		VolumeDir: "/var/lib/groundplane/vol/platform/" + projectID + "/" + hierarchyTestID(ids.KindEnvironment, 61),
		CreatedAt: time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CreateEnvironment(backing) error = %v, want validation.failed", err)
	}
}

func tenantIDs(items []Versioned[TenantRecord]) []string {
	result := make([]string, len(items))
	for index, item := range items {
		result[index] = item.Record.ID
	}
	return result
}

func seedHierarchyTest(t *testing.T, store *memoryHierarchyStore, mutations []Mutation) {
	t.Helper()
	if _, err := store.Transact(context.Background(), nil, mutations); err != nil {
		t.Fatalf("seed hierarchy store: %v", err)
	}
}

func assertInternalHierarchyError(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("read error = %v, want internal", err)
	}
}

func hierarchyTestID(kind ids.Kind, second int64) string {
	return ids.NewAt(kind, time.Unix(second, 0).UTC(), second)
}

type memoryVersion struct {
	revision int64
	value    []byte
	present  bool
}

type memoryHierarchyStore struct {
	revision int64
	history  map[string][]memoryVersion
}

type deletionRaceHierarchyStore struct {
	*memoryHierarchyStore
	tombstoneKey string
	injected     bool
}

func (store *deletionRaceHierarchyStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	result, err := store.memoryHierarchyStore.GetMany(ctx, request)
	if err != nil || store.injected || !containsHierarchyKey(request.Keys, store.tombstoneKey) {
		return result, err
	}
	store.injected = true
	_, err = store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: store.tombstoneKey, Value: []byte(`{"phase":"requested"}`),
	}})
	return result, err
}

func containsHierarchyKey(keys []string, target string) bool {
	for _, key := range keys {
		if key == target {
			return true
		}
	}
	return false
}

func newMemoryHierarchyStore() *memoryHierarchyStore {
	return &memoryHierarchyStore{history: make(map[string][]memoryVersion)}
}

func (store *memoryHierarchyStore) Get(_ context.Context, key string) (*GetResult, error) {
	value := store.valueAt(key, store.revision)
	return &GetResult{Entry: value, ReadRevision: store.revision}, nil
}

func (store *memoryHierarchyStore) GetMany(
	_ context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	values := make([]*KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueAt(key, revision)
	}
	return &GetManyResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision,
	}, nil
}

func (store *memoryHierarchyStore) Range(
	_ context.Context,
	request RangeRequest,
) (*RangeResult, error) {
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	keys := make([]string, 0)
	for key := range store.history {
		if !strings.HasPrefix(key, request.Prefix) || key <= request.StartExclusive {
			continue
		}
		if store.valueAt(key, revision) != nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	more := int64(len(keys)) > request.Limit
	if more {
		keys = keys[:request.Limit]
	}
	values := make([]KeyValue, 0, len(keys))
	for _, key := range keys {
		value := store.valueAt(key, revision)
		values = append(values, *value)
	}
	return &RangeResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision, More: more,
	}, nil
}

func (store *memoryHierarchyStore) Transact(
	_ context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	for _, condition := range conditions {
		value := store.valueAt(condition.Key, store.revision)
		actualRevision := int64(0)
		if value != nil {
			actualRevision = value.ModRevision
		}
		if actualRevision != condition.ModRevision {
			failureReads := make([]*KeyValue, len(conditions))
			for index, failedCondition := range conditions {
				failureReads[index] = store.valueAt(failedCondition.Key, store.revision)
			}
			return TransactionResult{
				Succeeded: false, Revision: store.revision, FailureReads: failureReads,
			}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		if mutation.Type == MutationDelete && mutation.Prefix {
			for key := range store.history {
				if strings.HasPrefix(key, mutation.Key) {
					store.history[key] = append(
						store.history[key],
						memoryVersion{revision: store.revision},
					)
				}
			}
			continue
		}
		version := memoryVersion{revision: store.revision}
		switch mutation.Type {
		case MutationPut:
			version.present = true
			version.value = append([]byte(nil), mutation.Value...)
		case MutationDelete:
		default:
			return TransactionResult{}, errs.New(errs.KindInternal, "fake store received invalid mutation")
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], version)
	}
	return TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryHierarchyStore) valueAt(key string, revision int64) *KeyValue {
	versions := store.history[key]
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		if version.revision > revision {
			continue
		}
		if !version.present {
			return nil
		}
		return &KeyValue{
			Key: key, Value: append([]byte(nil), version.value...), ModRevision: version.revision,
		}
	}
	return nil
}
