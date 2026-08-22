package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyEnvironmentIdempotentRenameMovesOnlyScopedName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 601), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 602), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	if _, err := repository.CreateProject(ctx, project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	createdAt := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
	record, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		project,
		hierarchyTestID(ids.KindEnvironment, 603),
		"production",
		hierarchyTestID(ids.KindTask, 604),
		createdAt,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	if _, err := repository.CreateEnvironment(ctx, record); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	current, err := repository.GetEnvironment(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	replacement := current.Record
	replacement.Name = "live"
	result, err := repository.MutateEnvironmentIdempotent(
		ctx,
		current,
		replacement,
		environmentMutationTestMarker(record.ID, "environment-rename-key-0001"),
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateEnvironmentIdempotent() = %#v, %v", result, err)
	}
	resolved, err := repository.ResolveEnvironment(ctx, project.ID, "live")
	if err != nil || resolved.Record.ID != record.ID || resolved.Record.VolumeDir != record.VolumeDir ||
		resolved.Record.CreateTaskID != record.CreateTaskID ||
		resolved.Record.ProvisioningState != record.ProvisioningState {
		t.Fatalf("ResolveEnvironment(new name) = %#v, %v", resolved.Record, err)
	}
	if _, err := repository.ResolveEnvironment(
		ctx,
		project.ID,
		"production",
	); !isKind(
		err,
		errs.KindEnvironmentNotFound,
	) {
		t.Fatalf("ResolveEnvironment(old name) error = %v", err)
	}
}

func TestHierarchyEnvironmentMutationRejectsProvisioningChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	createdAt := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	record := EnvironmentRecord{
		ID:        hierarchyTestID(ids.KindEnvironment, 605),
		ProjectID: hierarchyTestID(ids.KindProject, 606),
		Name:      "production",
		VolumeDir: environmentpath.DefaultVolumeRoot + "/platform/" + hierarchyTestID(
			ids.KindProject,
			606,
		) + "/" + hierarchyTestID(
			ids.KindEnvironment,
			605,
		),
		ProvisioningState: EnvironmentProvisioningProvisioning,
		CreateTaskID:      hierarchyTestID(ids.KindTask, 607),
		CreatedAt:         createdAt,
	}
	replacement := record
	replacement.ProvisioningState = EnvironmentProvisioningFailed
	_, err = repository.MutateEnvironmentIdempotent(
		ctx,
		Versioned[EnvironmentRecord]{Record: record, Revision: 1, ReadRevision: 1},
		replacement,
		environmentMutationTestMarker(record.ID, "environment-rename-key-0002"),
	)
	if !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("MutateEnvironmentIdempotent(provisioning change) error = %v", err)
	}
}

func environmentMutationTestMarker(environmentID string, key string) IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPost, Route: "/environments/{id}/rename", Key: key,
	}
	return marker
}
