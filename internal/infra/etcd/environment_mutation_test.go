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
		"10.30.0.0/16",
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
	record := EnvironmentRecord{NetworkPool: "10.40.0.0/16",
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

// Rationale: the rename-only repository seam must not allow a caller to
// bypass Zone containment and global reservation checks by changing the pool.
func TestHierarchyEnvironmentMutationRejectsNetworkPoolChanges(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 15, 30, 0, 0, time.UTC)
	record := EnvironmentRecord{
		ID:          hierarchyTestID(ids.KindEnvironment, 612),
		ProjectID:   hierarchyTestID(ids.KindProject, 613),
		Name:        "production",
		NetworkPool: "10.40.0.0/16",
		VolumeDir: environmentpath.DefaultVolumeRoot + "/platform/" + hierarchyTestID(
			ids.KindProject,
			613,
		) + "/" + hierarchyTestID(ids.KindEnvironment, 612),
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      hierarchyTestID(ids.KindTask, 614),
		CreatedAt:         createdAt,
	}
	replacement := record
	replacement.NetworkPool = "10.50.0.0/16"
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	_, err = repository.MutateEnvironmentIdempotent(
		context.Background(),
		Versioned[EnvironmentRecord]{Record: record, Revision: 1, ReadRevision: 1},
		replacement,
		environmentMutationTestMarker(record.ID, "environment-rename-key-0005"),
	)
	if !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("MutateEnvironmentIdempotent(network pool change) error = %v", err)
	}
}

// Rationale: an Environment rename is a descendant write and must remain
// forbidden for the full lifetime of its owning Tenant deletion fence.
func TestHierarchyEnvironmentRenameRejectsDeletingTenant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, tenant, current := environmentMutationTestHierarchy(t, newMemoryHierarchyStore())
	tombstoneKey := deletionTombstoneKey("tenant", tenant.ID)
	result, err := store.Transact(ctx, []Condition{{Key: tombstoneKey}}, []Mutation{{
		Type: MutationPut, Key: tombstoneKey, Value: []byte(`{"phase":"requested"}`),
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("create Tenant deletion tombstone = %#v, %v", result, err)
	}
	current, err = repository.GetEnvironment(ctx, current.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	replacement := current.Record
	replacement.Name = "live"
	_, err = repository.MutateEnvironmentIdempotent(
		ctx,
		current,
		replacement,
		environmentMutationTestMarker(current.Record.ID, "environment-rename-key-0003"),
	)
	if !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("MutateEnvironmentIdempotent(deleting Tenant) error = %v", err)
	}
}

// Rationale: a Tenant deletion that races an Environment rename must win the
// same transaction, write neither rename nor replay marker, and allow an exact
// retry and replay only after the deletion fence is removed.
func TestHierarchyEnvironmentRenameTenantDeletionRaceRetriesAndReplays(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := newMemoryHierarchyStore()
	racing := &deletionRaceHierarchyStore{memoryHierarchyStore: base}
	repository, _, tenant, current := environmentMutationTestHierarchy(t, racing)
	tombstoneKey := deletionTombstoneKey("tenant", tenant.ID)
	racing.tombstoneKey = tombstoneKey
	replacement := current.Record
	replacement.Name = "live"
	marker := environmentMutationTestMarker(current.Record.ID, "environment-rename-key-0004")

	result, err := repository.MutateEnvironmentIdempotent(ctx, current, replacement, marker)
	if err != nil || result.kind != idempotencyTransactionConflict ||
		!isKind(result.conflict, errs.KindResourceInUse) {
		t.Fatalf("MutateEnvironmentIdempotent(Tenant deletion race) = %#v, %v", result, err)
	}
	if !racing.injected {
		t.Fatal("Tenant deletion race was not injected")
	}
	if _, err := repository.ResolveEnvironment(
		ctx,
		current.Record.ProjectID,
		current.Record.Name,
	); err != nil {
		t.Fatalf("ResolveEnvironment(original after race) error = %v", err)
	}
	if _, err := repository.ResolveEnvironment(
		ctx,
		current.Record.ProjectID,
		replacement.Name,
	); !isKind(err, errs.KindEnvironmentNotFound) {
		t.Fatalf("ResolveEnvironment(replacement after race) error = %v", err)
	}

	transaction, err := base.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: tombstoneKey}})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("remove Tenant deletion tombstone = %#v, %v", transaction, err)
	}
	result, err = repository.MutateEnvironmentIdempotent(ctx, current, replacement, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("MutateEnvironmentIdempotent(retry) = %#v, %v", result, err)
	}
	replayed, err := repository.MutateEnvironmentIdempotent(ctx, current, replacement, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("MutateEnvironmentIdempotent(replay) = %#v, %v", replayed, err)
	}
}

func environmentMutationTestHierarchy(
	t *testing.T,
	store hierarchyStore,
) (*HierarchyRepository, *memoryHierarchyStore, TenantRecord, Versioned[EnvironmentRecord]) {
	t.Helper()
	base, ok := store.(*memoryHierarchyStore)
	if !ok {
		if racing, racingOK := store.(*deletionRaceHierarchyStore); racingOK {
			base = racing.memoryHierarchyStore
		}
	}
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 608), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 609), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	if _, err := repository.CreateProject(context.Background(), project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	record, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		project,
		hierarchyTestID(ids.KindEnvironment, 610),
		"production",
		"10.50.0.0/16",
		hierarchyTestID(ids.KindTask, 611),
		time.Date(2026, 8, 22, 16, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	if _, err := repository.CreateEnvironment(context.Background(), record); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	current, err := repository.GetEnvironment(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	return repository, base, tenant, current
}

func environmentMutationTestMarker(environmentID string, key string) IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPost, Route: "/environments/{id}/rename", Key: key,
	}
	return marker
}
