package etcd

import (
	"context"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyCreateTenantIdempotentCommitsResourceIndexesAndMarker(t *testing.T) {
	t.Parallel()

	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	record := TenantRecord{
		ID: hierarchyTestID(ids.KindTenant, 201), Slug: "acme", Name: "Acme",
		Description: "Production workloads",
	}
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost,
		Route: "/tenants", Key: "tenant-create-key-0001",
	}
	marker.Response = IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: []byte(`{"id":"tenant"}`),
	}

	result, err := repository.CreateTenantIdempotent(context.Background(), record, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateTenantIdempotent() = %#v, %v", result, err)
	}
	stored, err := repository.GetTenant(context.Background(), record.ID)
	if err != nil || stored.Record != record {
		t.Fatalf("GetTenant() = %#v, %v", stored, err)
	}
	replayed, err := repository.CreateTenantIdempotent(context.Background(), record, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("CreateTenantIdempotent(replay) = %#v, %v", replayed, err)
	}

	conflictingRecord := record
	conflictingRecord.ID = hierarchyTestID(ids.KindTenant, 202)
	conflictingMarker := marker
	conflictingMarker.Locator.Key = "tenant-create-key-0002"
	conflict, err := repository.CreateTenantIdempotent(
		context.Background(), conflictingRecord, conflictingMarker,
	)
	if err != nil || conflict.kind != idempotencyTransactionConflict ||
		!isKind(conflict.conflict, errs.KindSlugConflict) {
		t.Fatalf("CreateTenantIdempotent(conflict) = %#v, %v", conflict, err)
	}
	if _, err := repository.GetTenant(
		context.Background(),
		conflictingRecord.ID,
	); !isKind(
		err,
		errs.KindTenantNotFound,
	) {
		t.Fatalf("conflicting Tenant persisted: %v", err)
	}
}

func TestHierarchyMutateTenantIdempotentCommitsEditsAndSlugMoves(t *testing.T) {
	t.Parallel()

	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	record := TenantRecord{
		ID: hierarchyTestID(ids.KindTenant, 211), Slug: "acme", Name: "Acme",
	}
	created, err := repository.CreateTenant(context.Background(), record)
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	editMarker := testDirectMarker()
	editMarker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPatch,
		Route: "/tenants/{id}", Key: "tenant-edit-key-0001",
	}
	editMarker.Response = IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"name":"Acme Inc"}`),
	}
	editedRecord := record
	editedRecord.Name = "Acme Inc"
	edited, err := repository.MutateTenantIdempotent(context.Background(), created, editedRecord, editMarker)
	if err != nil || edited.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateTenantIdempotent(edit) = %#v, %v", edited, err)
	}
	current, err := repository.GetTenant(context.Background(), record.ID)
	if err != nil || current.Record != editedRecord {
		t.Fatalf("GetTenant(edited) = %#v, %v", current, err)
	}

	renameMarker := testDirectMarker()
	renameMarker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost,
		Route: "/tenants/{id}/rename", Key: "tenant-rename-key-0001",
	}
	renameMarker.Response = IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"slug":"acme-inc"}`),
	}
	renamedRecord := editedRecord
	renamedRecord.Slug = "acme-inc"
	renamed, err := repository.MutateTenantIdempotent(context.Background(), current, renamedRecord, renameMarker)
	if err != nil || renamed.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateTenantIdempotent(rename) = %#v, %v", renamed, err)
	}
	resolved, err := repository.ResolveTenant(context.Background(), "acme-inc")
	if err != nil || resolved.Record != renamedRecord {
		t.Fatalf("ResolveTenant(new slug) = %#v, %v", resolved, err)
	}
	if _, err := repository.ResolveTenant(context.Background(), "acme"); !isKind(err, errs.KindTenantNotFound) {
		t.Fatalf("ResolveTenant(old slug) error = %v", err)
	}
	replayed, err := repository.MutateTenantIdempotent(
		context.Background(), current, renamedRecord, renameMarker,
	)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("MutateTenantIdempotent(replay) = %#v, %v", replayed, err)
	}
}
