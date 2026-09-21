package etcd

import (
	"context"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyProjectIdempotentCreateCommitsIndexesMarkerAndOwnerFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := testhierarchy.TenantRecord{ID: hierarchyTestID(ids.KindTenant, 401), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	record := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 402), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Description: "Operator interface", Kind: testhierarchy.ProjectKindTenant,
	}
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeTenant, ScopeID: tenant.ID,
		Method: http.MethodPost, Route: "/projects", Key: "project-create-key-0001",
	}
	marker.Response.Status = http.StatusCreated
	if _, err := repository.CreateProjectIdempotent(ctx, record, marker); err != nil {
		t.Fatalf("CreateProjectIdempotent() error = %v", err)
	}
	stored, err := repository.GetProject(ctx, record.ID)
	if err != nil || stored.Record != record {
		t.Fatalf("GetProject() = %#v, %v", stored.Record, err)
	}
	assertHierarchyCoordinationRecord(
		t,
		store,
		testhierarchydeletion.HierarchyDeletionTargetProject,
		record.ID,
		stored.Revision,
	)
	if _, err := repository.CreateProjectIdempotent(ctx, record, marker); err != nil {
		t.Fatalf("CreateProjectIdempotent(replay) error = %v", err)
	}

	conflict := record
	conflict.ID = hierarchyTestID(ids.KindProject, 403)
	conflictMarker := marker
	conflictMarker.Locator.Key = "project-create-key-0002"
	conflictResult, err := repository.CreateProjectIdempotent(ctx, conflict, conflictMarker)
	if err != nil || conflictResult.kind != idempotencyTransactionConflict ||
		!isKind(conflictResult.conflict, errs.KindSlugConflict) {
		t.Fatalf("CreateProjectIdempotent(slug conflict) result/error = %#v, %v", conflictResult, err)
	}

	tombstoneKey := testdeletions.TombstoneKey("tenant", tenant.ID)
	result, err := store.Transact(ctx, []testkeyvalue.Condition{{Key: tombstoneKey}}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: tombstoneKey, Value: []byte("deleting")},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("create Tenant tombstone = %#v, %v", result, err)
	}
	blocked := record
	blocked.ID = hierarchyTestID(ids.KindProject, 404)
	blocked.Slug = "api"
	blockedMarker := marker
	blockedMarker.Locator.Key = "project-create-key-0003"
	if _, err := repository.CreateProjectIdempotent(ctx, blocked, blockedMarker); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("CreateProjectIdempotent(deleting owner) error = %v", err)
	}
}
