package etcd

import (
	"context"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyProjectIdempotentMutationPreservesOwnerAndMovesOnlyScopedSlug(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := testhierarchy.TenantRecord{ID: hierarchyTestID(ids.KindTenant, 501), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	record := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 502), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Description: "Create-time summary", Kind: testhierarchy.ProjectKindTenant,
	}
	if _, err := repository.CreateProject(ctx, record); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	current, err := repository.GetProject(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}
	replacement := current.Record
	replacement.Name = "Operator Console"
	marker := projectMutationTestMarker(record.ID, "project-edit-key-0001", http.MethodPatch, "/projects/{id}")
	result, err := repository.MutateProjectIdempotent(ctx, current, replacement, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateProjectIdempotent(edit) = %#v, %v", result, err)
	}
	current, err = repository.GetProject(ctx, record.ID)
	if err != nil || current.Record.Name != "Operator Console" ||
		current.Record.Description != record.Description || current.Record.TenantID != tenant.ID {
		t.Fatalf("edited Project = %#v, %v", current.Record, err)
	}
	replacement = current.Record
	replacement.Slug = "operator-console"
	marker = projectMutationTestMarker(record.ID, "project-rename-key-0001", http.MethodPost, "/projects/{id}/rename")
	result, err = repository.MutateProjectIdempotent(ctx, current, replacement, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateProjectIdempotent(rename) = %#v, %v", result, err)
	}
	resolved, err := repository.ResolveTenantProject(ctx, tenant.ID, "operator-console")
	if err != nil || resolved.Record.ID != record.ID || resolved.Record.Description != record.Description {
		t.Fatalf("ResolveTenantProject(new slug) = %#v, %v", resolved.Record, err)
	}
	if _, err := repository.ResolveTenantProject(ctx, tenant.ID, "console"); !isKind(err, errs.KindProjectNotFound) {
		t.Fatalf("ResolveTenantProject(old slug) error = %v", err)
	}
}

func projectMutationTestMarker(
	projectID string,
	key string,
	method string,
	route string,
) testidempotency.IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeProject, ScopeID: projectID,
		Method: method, Route: route, Key: key,
	}
	return marker
}
