package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestServiceRepositoryCreatesReadsAndPagesScopedRecords(t *testing.T) {
	// Rationale: one atomic write must publish the Service primary, scoped name,
	// and owner index consumed by stable-id reads and fixed-revision listing.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := serviceRepositoryTestHierarchy(t)
	records := []ServiceRecord{
		serviceRepositoryTestRecord(t, environment.Record.ID, 810, "api"),
		serviceRepositoryTestRecord(t, environment.Record.ID, 811, "worker"),
	}
	for _, record := range records {
		if _, err := repository.CreateService(ctx, environment, project, record); err != nil {
			t.Fatalf("CreateService(%s) error = %v", record.Desired.Name, err)
		}
	}
	stored, err := repository.GetService(ctx, records[0].Desired.ID)
	if err != nil || stored.Record.Desired.Name != "api" ||
		stored.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("GetService() = %#v, %v", stored, err)
	}
	first, err := repository.ListServices(ctx, environment.Record.ID, PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("ListServices(first) = %#v, %v", first, err)
	}
	second, err := repository.ListServices(
		ctx,
		environment.Record.ID,
		PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision {
		t.Fatalf("ListServices(second) = %#v, %v", second, err)
	}
}

func TestServiceRepositoryEnforcesScopedNameAndAncestorFences(t *testing.T) {
	// Rationale: direct create and Blueprint reconciliation must share one
	// atomic uniqueness rule and cannot create descendants under deletion.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project := serviceRepositoryTestHierarchy(t)
	first := serviceRepositoryTestRecord(t, environment.Record.ID, 820, "api")
	if _, err := repository.CreateService(ctx, environment, project, first); err != nil {
		t.Fatalf("CreateService(first) error = %v", err)
	}
	duplicate := serviceRepositoryTestRecord(t, environment.Record.ID, 821, "api")
	if _, err := repository.CreateService(ctx, environment, project, duplicate); !isKind(err, errs.KindNameConflict) {
		t.Fatalf("CreateService(duplicate) error = %v", err)
	}
	if _, err := store.Transact(ctx,
		[]Condition{{Key: deletionTombstoneKey("environment", environment.Record.ID)}},
		[]Mutation{{
			Type: MutationPut, Key: deletionTombstoneKey("environment", environment.Record.ID), Value: []byte("fenced"),
		}},
	); err != nil {
		t.Fatalf("install Environment fence: %v", err)
	}
	fenced := serviceRepositoryTestRecord(t, environment.Record.ID, 822, "scheduler")
	if _, err := repository.CreateService(ctx, environment, project, fenced); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("CreateService(fenced) error = %v", err)
	}
}

func TestServiceRepositoryUpdatesDesiredAndRuntimeByCAS(t *testing.T) {
	// Rationale: concurrent Blueprint and lifecycle writes must serialize on one
	// Service revision without either authorship domain overwriting the other.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := serviceRepositoryTestHierarchy(t)
	record := serviceRepositoryTestRecord(t, environment.Record.ID, 830, "api")
	current, err := repository.CreateService(ctx, environment, project, record)
	if err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	stopped, err := repository.SetRuntimeIntent(
		ctx, environment, project, current, core.ServiceRuntimeIntentStopped,
	)
	if err != nil {
		t.Fatalf("SetRuntimeIntent() error = %v", err)
	}
	desired := stopped.Record.Desired
	desired.Image = "app:next"
	updated, err := repository.ReplaceDesired(ctx, environment, project, stopped, desired)
	if err != nil {
		t.Fatalf("ReplaceDesired() error = %v", err)
	}
	if updated.Record.Desired.Image != "app:next" ||
		updated.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentStopped {
		t.Fatalf("updated record = %#v", updated.Record)
	}
	if _, err := repository.SetRuntimeIntent(
		ctx, environment, project, current, core.ServiceRuntimeIntentAbsent,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("SetRuntimeIntent(stale) error = %v", err)
	}
}

func serviceRepositoryTestHierarchy(
	t *testing.T,
) (*ServiceRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) {
	t.Helper()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	repository, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 800), Slug: "acme", Name: "Acme"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projectRecord := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 801), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	project, err := hierarchy.CreateProject(ctx, projectRecord)
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentRecord, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		projectRecord,
		hierarchyTestID(ids.KindEnvironment, 802),
		"production",
		hierarchyTestID(ids.KindTask, 803),
		serviceRecordTestTime(),
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	environment, err := hierarchy.CreateEnvironment(ctx, environmentRecord)
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	return repository, store, environment, project
}

func serviceRepositoryTestRecord(
	t *testing.T,
	environmentID string,
	offset int64,
	name string,
) ServiceRecord {
	t.Helper()
	record, err := NewServiceRecord(environmentID, core.Service{
		ID:   ids.NewAt(ids.KindService, serviceRecordTestTime(), offset),
		Name: name, Image: "app:latest", Strategy: core.StrategyRecreate,
	}, "")
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	return record
}
