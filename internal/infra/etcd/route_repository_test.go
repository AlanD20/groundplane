package etcd

import (
	"context"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRouteRepositoryCreatesReadsAndPagesScopedRecords(t *testing.T) {
	// Rationale: one atomic write must publish the Route primary and Environment
	// membership while fencing the exact live target Service.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project, target := routeRepositoryTestHierarchy(t)
	records := []RouteRecord{
		routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1010, "/api/*"),
		routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1011, "/admin/*"),
	}
	for _, record := range records {
		if _, err := repository.CreateRoute(ctx, environment, project, target, record); err != nil {
			t.Fatalf("CreateRoute(%s) error = %v", record.Desired.Path, err)
		}
	}
	duplicateMatch := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1012, "/api/*")
	if _, err := repository.CreateRoute(ctx, environment, project, target, duplicateMatch); !isKind(
		err,
		errs.KindNameConflict,
	) {
		t.Fatalf("CreateRoute(duplicate match) error = %v", err)
	}
	stored, err := repository.GetRoute(ctx, records[0].Desired.ID)
	if err != nil || stored.Record != records[0] {
		t.Fatalf("GetRoute() = %#v, %v", stored, err)
	}
	first, err := repository.ListRoutes(ctx, environment.Record.ID, PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("ListRoutes(first) = %#v, %v", first, err)
	}
	second, err := repository.ListRoutes(
		ctx,
		environment.Record.ID,
		PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision {
		t.Fatalf("ListRoutes(second) = %#v, %v", second, err)
	}
}

func TestRouteRepositoryCommitsIdempotentCreateAndExposureEdit(t *testing.T) {
	// Rationale: a synchronous Route response must never exist without the
	// exact durable mutation, indexes, and replay marker from the same txn.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1005, "/api/*")
	createMarker := testDirectMarker()
	createMarker.Locator.ScopeKind = IdempotencyScopeEnvironment
	createMarker.Locator.ScopeID = environment.Record.ID
	createMarker.Locator.Method = http.MethodPost
	createMarker.Locator.Route = "/routes"
	createMarker.Locator.Key = "route-create-key-0001"
	if _, err := repository.CreateRouteIdempotent(
		ctx, environment, project, target, record, createMarker,
	); err != nil {
		t.Fatalf("CreateRouteIdempotent() error = %v", err)
	}
	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil || current.Record != record {
		t.Fatalf("GetRoute(created) = %#v, %v", current, err)
	}
	desired := current.Record.Desired
	desired.Exposure = "internal"
	editMarker := testDirectMarker()
	editMarker.Locator.ScopeKind = IdempotencyScopeEnvironment
	editMarker.Locator.ScopeID = environment.Record.ID
	editMarker.Locator.Method = http.MethodPatch
	editMarker.Locator.Route = "/routes/{id}"
	editMarker.Locator.Key = "route-edit-key-000001"
	if _, err := repository.ReplaceDesiredIdempotent(
		ctx, environment, project, target, current, desired, editMarker,
	); err != nil {
		t.Fatalf("ReplaceDesiredIdempotent() error = %v", err)
	}
	updated, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil || updated.Record.Desired.Exposure != "internal" ||
		updated.Record.Desired.Host != record.Desired.Host || updated.Record.Desired.Path != record.Desired.Path ||
		updated.Record.Desired.TargetServiceID != record.Desired.TargetServiceID ||
		updated.Record.Desired.TargetPort != record.Desired.TargetPort {
		t.Fatalf("GetRoute(edited) = %#v, %v", updated, err)
	}
}

func TestRouteRepositoryRejectsMissingOrDeletingTargetService(t *testing.T) {
	// Rationale: required Route validation must stay true across the commit, not
	// merely observe a target Service before a concurrent deletion.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	wrongTarget := target
	wrongTarget.Record.Desired.ID = ids.NewAt(ids.KindService, serviceRecordTestTime(), 1021)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1020, "/api/*")
	if _, err := repository.CreateRoute(ctx, environment, project, wrongTarget, record); err == nil {
		t.Fatal("CreateRoute() accepted the wrong target Service")
	}
	if _, err := store.Transact(
		ctx,
		[]Condition{{Key: deletionTombstoneKey("service", target.Record.Desired.ID)}},
		[]Mutation{{
			Type: MutationPut, Key: deletionTombstoneKey("service", target.Record.Desired.ID), Value: []byte("fenced"),
		}},
	); err != nil {
		t.Fatalf("install Service fence: %v", err)
	}
	if _, err := repository.CreateRoute(
		ctx,
		environment,
		project,
		target,
		record,
	); !isKind(
		err,
		errs.KindResourceInUse,
	) {
		t.Fatalf("CreateRoute(deleting target) error = %v", err)
	}
}

func TestRouteRepositoryUpdatesExposureByCAS(t *testing.T) {
	// Rationale: direct and Blueprint exposure edits must serialize without
	// changing the typed match or target Service.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1030, "/api/*")
	current, err := repository.CreateRoute(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	desired := current.Record.Desired
	desired.Exposure = "internal"
	updated, err := repository.ReplaceDesired(ctx, environment, project, target, current, desired)
	if err != nil || updated.Record.Desired.Exposure != "internal" {
		t.Fatalf("ReplaceDesired() = %#v, %v", updated, err)
	}
	desired = updated.Record.Desired
	desired.Exposure = "public"
	updatedAgain, err := repository.ReplaceDesired(ctx, environment, project, target, updated, desired)
	if err != nil || updatedAgain.Record.Desired.Exposure != "public" {
		t.Fatalf("ReplaceDesired(second) = %#v, %v", updatedAgain, err)
	}
	if _, err := repository.ReplaceDesired(
		ctx,
		environment,
		project,
		target,
		current,
		desired,
	); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("ReplaceDesired(stale) error = %v", err)
	}
}

func routeRepositoryTestHierarchy(
	t *testing.T,
) (
	*RouteRepository,
	*memoryHierarchyStore,
	Versioned[EnvironmentRecord],
	Versioned[ProjectRecord],
	Versioned[ServiceRecord],
) {
	t.Helper()
	serviceRepository, store, environment, project := serviceRepositoryTestHierarchy(t)
	targetRecord := serviceRepositoryTestRecord(t, environment.Record.ID, 1004, "api")
	targetRecord.Desired.Replicas = 1
	target, err := serviceRepository.CreateService(context.Background(), environment, project, targetRecord)
	if err != nil {
		t.Fatalf("CreateService(target) error = %v", err)
	}
	repository, err := newRouteRepository(store)
	if err != nil {
		t.Fatalf("newRouteRepository() error = %v", err)
	}
	return repository, store, environment, project, target
}

func routeRepositoryTestRecord(
	t *testing.T,
	environmentID string,
	serviceID string,
	offset int64,
	path string,
) RouteRecord {
	t.Helper()
	record, err := NewRouteRecord(environmentID, core.Route{
		ID: ids.NewAt(ids.KindRoute, serviceRecordTestTime(), offset), Host: "app.example.com",
		Path: path, TargetServiceID: serviceID, TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	return record
}
