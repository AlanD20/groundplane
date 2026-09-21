package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskRepositoryListsImmutableOwnerScopesAtFixedRevision(t *testing.T) {
	// Rationale: workspace and Environment journals must read only their
	// immutable owner indexes, preserve Task-id order, and hold one revision
	// across continuation pages while unrelated Tasks are published.
	ctx := context.Background()
	store := newMemoryTaskStore()
	at := taskJournalTime()
	tenantID := ids.NewAt(ids.KindTenant, at, 41)
	projectID := ids.NewAt(ids.KindProject, at, 42)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 43)

	platform := scopedTaskRecord(at, 51, testtaskjournal.PlatformTaskOwner())
	tenant := scopedTaskRecord(at.Add(time.Second), 52, testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
		TenantID:      tenantID,
	})
	environment := scopedTaskRecord(at.Add(2*time.Second), 53, testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
		TenantID:      tenantID,
		ProjectID:     projectID,
		EnvironmentID: environmentID,
	})
	for _, task := range []TaskRecord{platform, tenant, environment} {
		seedTaskRepositoryTask(t, store, task)
	}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}

	assertScopedTaskIDs(
		t,
		repository,
		TaskListScope{Kind: TaskListScopeGlobal},
		platform.ID,
		tenant.ID,
		environment.ID,
	)
	assertScopedTaskIDs(t, repository, TaskListScope{Kind: TaskListScopePlatformWorkspace}, platform.ID)
	assertScopedTaskIDs(
		t,
		repository,
		TaskListScope{Kind: TaskListScopeTenantWorkspace, ID: tenantID},
		tenant.ID,
		environment.ID,
	)
	assertScopedTaskIDs(
		t,
		repository,
		TaskListScope{Kind: TaskListScopeProject, ID: projectID},
		environment.ID,
	)
	assertScopedTaskIDs(
		t,
		repository,
		TaskListScope{Kind: TaskListScopeEnvironment, ID: environmentID},
		environment.ID,
	)

	first, err := repository.ListTasksByScope(
		ctx,
		TaskListScope{Kind: TaskListScopeTenantWorkspace, ID: tenantID}, testkeyvalue.PageRequest{Limit: 1},
	)
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.ID != tenant.ID || first.NextCursor == "" {
		t.Fatalf("tenant first page = %#v, %v", first, err)
	}
	late := scopedTaskRecord(at.Add(3*time.Second), 54, testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
		TenantID:      tenantID,
	})
	seedTaskRepositoryTask(t, store, late)
	second, err := repository.ListTasksByScope(
		ctx,
		TaskListScope{
			Kind: TaskListScopeTenantWorkspace,
			ID:   tenantID,
		},
		testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.Items[0].Record.ID != environment.ID ||
		second.Revision != first.Revision || second.NextCursor != "" {
		t.Fatalf("tenant second page = %#v, %v", second, err)
	}
	if _, err := repository.ListTasksByScope(
		ctx,
		TaskListScope{Kind: TaskListScopeEnvironment, ID: environmentID}, testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor},
	); !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
		t.Fatalf("cross-scope cursor error = %v, want malformed request", err)
	}
}

func TestTaskRepositoryScopedListRejectsInvalidScopesAndOwnerMismatch(t *testing.T) {
	// Rationale: invalid API scopes are validation failures, while an index
	// pointing at a Task owned elsewhere is durable corruption and never a skip.
	ctx := context.Background()
	store := newMemoryTaskStore()
	at := taskJournalTime()
	task := scopedTaskRecord(at, 61, testtaskjournal.PlatformTaskOwner())
	seedTaskRepositoryTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	invalid := []TaskListScope{
		{Kind: TaskListScopeGlobal, ID: "unexpected"},
		{Kind: TaskListScopePlatformWorkspace, ID: "unexpected"},
		{Kind: TaskListScopeTenantWorkspace, ID: "tenant-invalid"},
		{Kind: TaskListScopeProject, ID: "project-invalid"},
		{Kind: TaskListScopeEnvironment, ID: "env-invalid"},
		{Kind: TaskListScopeKind("unknown")},
	}
	for _, scope := range invalid {
		if _, err := repository.ListTasksByScope(ctx, scope, testkeyvalue.PageRequest{}); !errors.Is(
			err,
			errs.New(errs.KindValidationFailed, ""),
		) {
			t.Fatalf("ListTasksByScope(%#v) error = %v, want validation.failed", scope, err)
		}
	}

	tenantID := ids.NewAt(ids.KindTenant, at, 62)
	indexKey := testtaskjournal.TaskWorkspaceTenantIndexKey(tenantID, task.ID)
	result, err := store.Transact(ctx, []testkeyvalue.Condition{{Key: indexKey}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: indexKey, Value: []byte(task.ID),
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed mismatched owner index = %#v, %v", result, err)
	}
	if _, err := repository.ListTasksByScope(
		ctx,
		TaskListScope{Kind: TaskListScopeTenantWorkspace, ID: tenantID}, testkeyvalue.PageRequest{},
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("mismatched owner index error = %v, want internal", err)
	}

	companionStore := newMemoryTaskStore()
	projectID := ids.NewAt(ids.KindProject, at, 63)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 64)
	environmentTask := scopedTaskRecord(at.Add(time.Second), 65, testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
		TenantID:      tenantID,
		ProjectID:     projectID,
		EnvironmentID: environmentID,
	})
	seedTaskRepositoryTask(t, companionStore, environmentTask)
	companionKeys, err := taskOwnerIndexKeys(environmentTask.Owner, environmentTask.ID)
	if err != nil || len(companionKeys) != 2 {
		t.Fatalf("Environment Task owner keys = %v, %v", companionKeys, err)
	}
	deleted, err := companionStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationDelete, Key: companionKeys[0],
	}})
	if err != nil || !deleted.Succeeded {
		t.Fatalf("delete workspace companion index = %#v, %v", deleted, err)
	}
	companionRepository, err := newTaskRepository(companionStore)
	if err != nil {
		t.Fatalf("newTaskRepository(companion) error = %v", err)
	}
	if _, err := companionRepository.ListTasksByScope(
		ctx,
		TaskListScope{Kind: TaskListScopeEnvironment, ID: environmentID}, testkeyvalue.PageRequest{},
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("missing companion owner index error = %v, want internal", err)
	}
}

func TestTaskRepositoryScopedListsBatchDefaultAndMaximumPages(t *testing.T) {
	// Rationale: a maximum page and an Environment-owned default page exceed
	// the store's 96-operation bound once primaries and companion owner indexes
	// are read. Every batch must stay bounded without changing page order.
	ctx := context.Background()
	store := newMemoryTaskStore()
	at := taskJournalTime()
	tenantID := ids.NewAt(ids.KindTenant, at, 71)
	projectID := ids.NewAt(ids.KindProject, at, 72)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 73)
	environmentOwner := testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
		TenantID:      tenantID,
		ProjectID:     projectID,
		EnvironmentID: environmentID,
	}
	for index := range testkeyvalue.MaximumPageLimit {
		platformAt := at.Add(time.Duration(index) * time.Millisecond)
		environmentAt := at.Add(time.Duration(testkeyvalue.MaximumPageLimit+index) * time.Millisecond)
		seedTaskRepositoryTask(
			t,
			store,
			scopedTaskRecord(platformAt, int64(1000+index), testtaskjournal.PlatformTaskOwner()),
		)
		seedTaskRepositoryTask(t, store, scopedTaskRecord(environmentAt, int64(2000+index), environmentOwner))
	}
	audited := &boundedGetManyTaskStore{memoryTaskStore: store}
	repository, err := newTaskRepository(audited)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}

	defaultEnvironment, err := repository.ListTasksByScope(
		ctx,
		TaskListScope{Kind: TaskListScopeEnvironment, ID: environmentID}, testkeyvalue.PageRequest{},
	)
	if err != nil || len(defaultEnvironment.Items) != testkeyvalue.DefaultPageLimit ||
		defaultEnvironment.NextCursor == "" {
		t.Fatalf(
			"default Environment page = %d items, cursor %q, %v",
			len(defaultEnvironment.Items),
			defaultEnvironment.NextCursor,
			err,
		)
	}

	scopes := []TaskListScope{
		{Kind: TaskListScopeGlobal},
		{Kind: TaskListScopePlatformWorkspace},
		{Kind: TaskListScopeTenantWorkspace, ID: tenantID},
		{Kind: TaskListScopeProject, ID: projectID},
		{Kind: TaskListScopeEnvironment, ID: environmentID},
	}
	for _, scope := range scopes {
		page, err := repository.ListTasksByScope(
			ctx,
			scope,
			testkeyvalue.PageRequest{Limit: testkeyvalue.MaximumPageLimit},
		)
		if err != nil || len(page.Items) != testkeyvalue.MaximumPageLimit {
			t.Fatalf("maximum page for %#v = %d items, %v", scope, len(page.Items), err)
		}
		for index := 1; index < len(page.Items); index++ {
			if page.Items[index-1].Record.ID >= page.Items[index].Record.ID {
				t.Fatalf("maximum page for %#v is not in Task-id order", scope)
			}
		}
	}
	if audited.maximumKeys != testkeyvalue.MaximumOperations {
		t.Fatalf("largest GetMany batch = %d, want %d", audited.maximumKeys, testkeyvalue.MaximumOperations)
	}
}

type boundedGetManyTaskStore struct {
	*memoryTaskStore
	maximumKeys int
}

func (store *boundedGetManyTaskStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if len(request.Keys) > testkeyvalue.MaximumOperations {
		return nil, errs.New(errs.KindInternal, "GetMany exceeded the operation limit")
	}
	store.maximumKeys = max(store.maximumKeys, len(request.Keys))
	return store.memoryTaskStore.GetMany(ctx, request)
}

func scopedTaskRecord(at time.Time, seed int64, owner testtaskjournal.TaskOwner) TaskRecord {
	record := validTaskRecord(at)
	record.ID = ids.NewAt(ids.KindTask, at, seed)
	record.OperationID = ids.NewAt(ids.KindOperation, at, seed+100)
	record.Owner = owner
	return record
}

func assertScopedTaskIDs(
	t *testing.T,
	repository *TaskRepository,
	scope TaskListScope,
	want ...string,
) {
	t.Helper()
	page, err := repository.ListTasksByScope(context.Background(), scope, testkeyvalue.PageRequest{})
	if err != nil {
		t.Fatalf("ListTasksByScope(%#v) error = %v", scope, err)
	}
	if len(page.Items) != len(want) {
		t.Fatalf("ListTasksByScope(%#v) items = %#v, want %v", scope, page.Items, want)
	}
	for index, id := range want {
		if page.Items[index].Record.ID != id {
			t.Fatalf("ListTasksByScope(%#v) item %d = %s, want %s", scope, index, page.Items[index].Record.ID, id)
		}
	}
}
