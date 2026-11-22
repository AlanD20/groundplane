package monitoring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/core"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestInventoryPagesFixedRevisionAcrossEmptyAndBackingBranches(t *testing.T) {
	t.Parallel()

	fixture := newInventoryFixture(t)
	inventory, err := NewInventory(fixture, fixture)
	if err != nil {
		t.Fatal(err)
	}
	first, err := inventory.Page(context.Background(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision != fixture.revision || len(first.Items) != 2 || first.Next == "" {
		t.Fatalf("first Page() = revision %d, %d items, next %q", first.Revision, len(first.Items), first.Next)
	}
	assertInventoryTarget(t, first.Items[0], fixture.servicesByEnvironment[fixture.workloadEnvironment][0],
		metrics.Ownership{
			TenantID: fixture.tenantID, ProjectID: fixture.workloadProject,
			EnvironmentID: fixture.workloadEnvironment,
			ServiceID:     fixture.servicesByEnvironment[fixture.workloadEnvironment][0].Record.Desired.ID,
			Class:         metrics.OwnerWorkload,
		})
	assertInventoryTarget(t, first.Items[1], fixture.servicesByEnvironment[fixture.workloadEnvironment][1],
		metrics.Ownership{
			TenantID: fixture.tenantID, ProjectID: fixture.workloadProject,
			EnvironmentID: fixture.workloadEnvironment,
			ServiceID:     fixture.servicesByEnvironment[fixture.workloadEnvironment][1].Record.Desired.ID,
			Class:         metrics.OwnerWorkload,
		})

	second, err := inventory.Page(context.Background(), first.Next, 2)
	if err != nil {
		t.Fatal(err)
	}
	if second.Revision != fixture.revision || len(second.Items) != 2 || second.Next != "" {
		t.Fatalf("second Page() = revision %d, %d items, next %q", second.Revision, len(second.Items), second.Next)
	}
	assertInventoryTarget(t, second.Items[0], fixture.servicesByEnvironment[fixture.workloadEnvironment][2],
		metrics.Ownership{
			TenantID: fixture.tenantID, ProjectID: fixture.workloadProject,
			EnvironmentID: fixture.workloadEnvironment,
			ServiceID:     fixture.servicesByEnvironment[fixture.workloadEnvironment][2].Record.Desired.ID,
			Class:         metrics.OwnerWorkload,
		})
	assertInventoryTarget(t, second.Items[1], fixture.servicesByEnvironment[fixture.backingEnvironment][0],
		metrics.Ownership{
			ProjectID: fixture.backingProject, EnvironmentID: fixture.backingEnvironment,
			ServiceID: fixture.servicesByEnvironment[fixture.backingEnvironment][0].Record.Desired.ID,
			Class:     metrics.OwnerBacking,
		})
	if !fixture.emptyEnvironmentRead {
		t.Fatal("Page() did not advance through the empty Environment branch")
	}
	for index, request := range fixture.requests {
		if index == 0 {
			if request.Revision != 0 {
				t.Fatalf("initial request revision = %d, want a fresh snapshot", request.Revision)
			}
			continue
		}
		if request.Revision != fixture.revision {
			t.Fatalf("nested request %d revision = %d, want %d", index, request.Revision, fixture.revision)
		}
	}
}

func TestInventoryFailsClosedOnRevisionChangeAndExpiredOrMalformedCursor(t *testing.T) {
	t.Parallel()

	fixture := newInventoryFixture(t)
	inventory, err := NewInventory(fixture, fixture)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inventory.Page(
		context.Background(),
		string(make([]byte, inventoryCursorLimit+1)),
		1,
	); !errors.Is(
		err,
		errs.New(errs.KindMalformedRequest, ""),
	) {
		t.Fatalf("oversized cursor error = %v, want malformed request", err)
	}
	if len(fixture.requests) != 0 {
		t.Fatalf("malformed cursor made %d storage reads", len(fixture.requests))
	}

	fixture.serviceRevision = fixture.revision + 1
	if _, err := inventory.Page(context.Background(), "", 1); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("changed revision error = %v, want state conflict", err)
	}

	fixture = newInventoryFixture(t)
	inventory, err = NewInventory(fixture, fixture)
	if err != nil {
		t.Fatal(err)
	}
	first, err := inventory.Page(context.Background(), "", 1)
	if err != nil {
		t.Fatal(err)
	}
	fixture.expireServiceCursor = true
	if _, err := inventory.Page(
		context.Background(),
		first.Next,
		1,
	); !errors.Is(
		err,
		errs.New(errs.KindCursorExpired, ""),
	) {
		t.Fatalf("expired continuation error = %v, want cursor expired", err)
	}
	fixture.expireServiceCursor = false
	restarted, err := inventory.Page(context.Background(), "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.Items) != 1 ||
		restarted.Items[0].Service.Record.Desired.ID != first.Items[0].Service.Record.Desired.ID {
		t.Fatalf("restarted Page() = %#v, want a fresh traversal", restarted.Items)
	}
}

func TestInventoryInternalDeadlineReturnsAdvancedContinuation(t *testing.T) {
	t.Parallel()

	fixture := newInventoryFixture(t)
	inventory, err := NewInventory(fixture, fixture)
	if err != nil {
		t.Fatal(err)
	}
	fixture.deadlineEnvironment = fixture.backingEnvironment
	inventory.pageContext = func(parent context.Context) (context.Context, func()) {
		bounded, cancel := context.WithCancelCause(parent)
		fixture.expirePage = func() { cancel(context.DeadlineExceeded) }
		return bounded, func() { cancel(context.Canceled) }
	}

	beforeDeadline, err := inventory.Page(context.Background(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeDeadline.Items) != 3 || beforeDeadline.Next == "" {
		t.Fatalf("deadline Page() = %d items, next %q", len(beforeDeadline.Items), beforeDeadline.Next)
	}
	emptyReads := fixture.serviceReads[fixture.emptyEnvironment]
	workloadReads := fixture.serviceReads[fixture.workloadEnvironment]

	afterDeadline, err := inventory.Page(context.Background(), beforeDeadline.Next, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterDeadline.Items) != 1 || afterDeadline.Next != "" ||
		afterDeadline.Items[0].Ownership.Class != metrics.OwnerBacking {
		t.Fatalf("resumed Page() = %#v, next %q", afterDeadline.Items, afterDeadline.Next)
	}
	if fixture.serviceReads[fixture.emptyEnvironment] != emptyReads ||
		fixture.serviceReads[fixture.workloadEnvironment] != workloadReads {
		t.Fatalf(
			"resume reread completed branches: empty %d -> %d, workload %d -> %d",
			emptyReads,
			fixture.serviceReads[fixture.emptyEnvironment],
			workloadReads,
			fixture.serviceReads[fixture.workloadEnvironment],
		)
	}
}

func assertInventoryTarget(
	t *testing.T,
	got InventoryTarget,
	wantService etcdstore.Versioned[servicerecord.ServiceRecord],
	wantOwnership metrics.Ownership,
) {
	t.Helper()
	if got.Service.Record.Desired.ID != wantService.Record.Desired.ID ||
		got.Service.ReadRevision != wantService.ReadRevision {
		t.Fatalf("target Service = %#v, want %#v", got.Service, wantService)
	}
	if got.Ownership != wantOwnership {
		t.Fatalf("target Ownership = %#v, want %#v", got.Ownership, wantOwnership)
	}
}

type inventoryFixture struct {
	revision int64
	at       time.Time

	tenantID            string
	workloadProject     string
	backingProject      string
	emptyEnvironment    string
	workloadEnvironment string
	backingEnvironment  string

	projects              []etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	environmentsByProject map[string][]etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	servicesByEnvironment map[string][]etcdstore.Versioned[servicerecord.ServiceRecord]
	requests              []etcdstore.PageRequest
	emptyEnvironmentRead  bool
	serviceRevision       int64
	expireServiceCursor   bool
	serviceCursorLimit    int
	serviceReads          map[string]int
	deadlineEnvironment   string
	expirePage            func()
}

func newInventoryFixture(t *testing.T) *inventoryFixture {
	t.Helper()
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	fixture := &inventoryFixture{
		revision:              77,
		at:                    at,
		tenantID:              ids.NewAt(ids.KindTenant, at, 1),
		workloadProject:       ids.NewAt(ids.KindProject, at, 2),
		backingProject:        ids.NewAt(ids.KindProject, at, 3),
		emptyEnvironment:      ids.NewAt(ids.KindEnvironment, at, 4),
		workloadEnvironment:   ids.NewAt(ids.KindEnvironment, at, 5),
		backingEnvironment:    ids.NewAt(ids.KindEnvironment, at, 6),
		environmentsByProject: make(map[string][]etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]),
		servicesByEnvironment: make(map[string][]etcdstore.Versioned[servicerecord.ServiceRecord]),
		serviceReads:          make(map[string]int),
	}
	fixture.projects = []etcdstore.Versioned[hierarchyrecord.ProjectRecord]{
		versionedInventoryRecord(fixture.revision, hierarchyrecord.ProjectRecord{
			ID: fixture.workloadProject, TenantID: fixture.tenantID, Kind: hierarchyrecord.ProjectKindTenant,
		}),
		versionedInventoryRecord(fixture.revision, hierarchyrecord.ProjectRecord{
			ID: fixture.backingProject, Kind: hierarchyrecord.ProjectKindBacking,
		}),
	}
	fixture.environmentsByProject[fixture.workloadProject] = []etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
		versionedInventoryRecord(fixture.revision, hierarchyrecord.EnvironmentRecord{
			ID: fixture.emptyEnvironment, ProjectID: fixture.workloadProject,
		}),
		versionedInventoryRecord(fixture.revision, hierarchyrecord.EnvironmentRecord{
			ID: fixture.workloadEnvironment, ProjectID: fixture.workloadProject,
		}),
	}
	fixture.environmentsByProject[fixture.backingProject] = []etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
		versionedInventoryRecord(fixture.revision, hierarchyrecord.EnvironmentRecord{
			ID: fixture.backingEnvironment, ProjectID: fixture.backingProject,
		}),
	}
	fixture.servicesByEnvironment[fixture.emptyEnvironment] = nil
	fixture.servicesByEnvironment[fixture.workloadEnvironment] = []etcdstore.Versioned[servicerecord.ServiceRecord]{
		fixture.service(t, fixture.workloadEnvironment, 7, "alpha"),
		fixture.service(t, fixture.workloadEnvironment, 8, "bravo"),
		fixture.service(t, fixture.workloadEnvironment, 9, "charlie"),
	}
	fixture.servicesByEnvironment[fixture.backingEnvironment] = []etcdstore.Versioned[servicerecord.ServiceRecord]{
		fixture.service(t, fixture.backingEnvironment, 10, "database"),
	}
	return fixture
}

func (fixture *inventoryFixture) service(
	t *testing.T,
	environmentID string,
	seed int64,
	name string,
) etcdstore.Versioned[servicerecord.ServiceRecord] {
	t.Helper()
	record, err := servicerecord.NewServiceRecord(environmentID, core.Service{
		ID: ids.NewAt(ids.KindService, fixture.at, seed), Name: name, Image: "example/app:1",
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureLeaveActive, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	return versionedInventoryRecord(fixture.revision, record)
}

func versionedInventoryRecord[T any](revision int64, record T) etcdstore.Versioned[T] {
	return etcdstore.Versioned[T]{Record: record, Revision: revision - 1, ReadRevision: revision}
}

func (fixture *inventoryFixture) ListProjects(
	ctx context.Context,
	_ hierarchyrecord.ProjectFilter,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.ProjectRecord], error) {
	if err := ctx.Err(); err != nil {
		return etcdstore.Page[hierarchyrecord.ProjectRecord]{}, err
	}
	fixture.requests = append(fixture.requests, request)
	index, ok := inventoryFixtureIndex(request.Cursor, []string{"", "project-2"})
	if !ok {
		return etcdstore.Page[hierarchyrecord.ProjectRecord]{}, errs.New(
			errs.KindMalformedRequest,
			"invalid Project cursor",
		)
	}
	return inventoryFixturePage(fixture.revision, fixture.projects, index, request.Limit, "project-2"), nil
}

func (fixture *inventoryFixture) ListEnvironments(
	ctx context.Context,
	projectID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.EnvironmentRecord], error) {
	if err := ctx.Err(); err != nil {
		return etcdstore.Page[hierarchyrecord.EnvironmentRecord]{}, err
	}
	fixture.requests = append(fixture.requests, request)
	environments := fixture.environmentsByProject[projectID]
	index, ok := inventoryFixtureIndex(request.Cursor, []string{"", "environment-2"})
	if !ok || index >= len(environments) {
		return etcdstore.Page[hierarchyrecord.EnvironmentRecord]{}, errs.New(
			errs.KindMalformedRequest,
			"invalid Environment cursor",
		)
	}
	return inventoryFixturePage(fixture.revision, environments, index, request.Limit, "environment-2"), nil
}

func (fixture *inventoryFixture) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[servicerecord.ServiceRecord], error) {
	fixture.requests = append(fixture.requests, request)
	fixture.serviceReads[environmentID]++
	if fixture.deadlineEnvironment == environmentID && fixture.expirePage != nil {
		expire := fixture.expirePage
		fixture.deadlineEnvironment, fixture.expirePage = "", nil
		expire()
		<-ctx.Done()
		return etcdstore.Page[servicerecord.ServiceRecord]{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, err
	}
	if fixture.expireServiceCursor && request.Cursor != "" {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, errs.New(errs.KindCursorExpired, "snapshot compacted")
	}
	services := fixture.servicesByEnvironment[environmentID]
	if len(services) == 0 {
		fixture.emptyEnvironmentRead = true
		return etcdstore.Page[servicerecord.ServiceRecord]{Revision: fixture.revision}, nil
	}
	index := 0
	if request.Cursor == "service-3" {
		if request.Limit != fixture.serviceCursorLimit {
			return etcdstore.Page[servicerecord.ServiceRecord]{}, errs.New(
				errs.KindMalformedRequest,
				"Service cursor changed its page size",
			)
		}
		index = 2
	} else if request.Cursor != "" {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindMalformedRequest,
			"invalid Service cursor",
		)
	}
	revision := fixture.revision
	if fixture.serviceRevision != 0 {
		revision = fixture.serviceRevision
	}
	page := inventoryFixturePage(revision, services, index, request.Limit, "service-3")
	if page.NextCursor != "" {
		fixture.serviceCursorLimit = request.Limit
	}
	return page, nil
}

func inventoryFixtureIndex(cursor string, cursors []string) (int, bool) {
	for index, candidate := range cursors {
		if candidate == cursor {
			return index, true
		}
	}
	return 0, false
}

func inventoryFixturePage[T any](
	revision int64,
	items []etcdstore.Versioned[T],
	index int,
	limit int,
	nextCursor string,
) etcdstore.Page[T] {
	end := min(index+limit, len(items))
	page := etcdstore.Page[T]{Items: append([]etcdstore.Versioned[T](nil), items[index:end]...), Revision: revision}
	if end < len(items) {
		page.NextCursor = nextCursor
	}
	return page
}
