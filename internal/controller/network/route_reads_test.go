package network

import (
	"context"
	"reflect"
	"testing"

	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
)

type fakeRouteReadRepository struct {
	routeReadRepository
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	route       testkeyvalue.Versioned[testroutes.Record]
	page        testkeyvalue.Page[testroutes.Record]
	wantRequest testkeyvalue.PageRequest
	listed      bool
}

func (fake *fakeRouteReadRepository) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeRouteReadRepository) GetRoute(
	context.Context, string,

) (testkeyvalue.Versioned[testroutes.Record], error) {
	return fake.route, nil
}

func (fake *fakeRouteReadRepository) ListRoutes(
	_ context.Context,
	environmentID string,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testroutes.Record], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

func TestRouteReadsVerifyOwnerAndPreserveDurableResults(t *testing.T) {
	// Rationale: Route collections are Environment-scoped while detail reads
	// preserve the exact stable target and match stored by the Controller.
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	routeID := "rte_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := testkeyvalue.PageRequest{Limit: 17, Cursor: "opaque"}
	wantRoute := testkeyvalue.Versioned[testroutes.Record]{
		Record: testroutes.Record{EnvironmentID: environmentID}, Revision: 19, ReadRevision: 19,
	}
	wantRoute.Record.Desired.ID = routeID
	want := testkeyvalue.Page[testroutes.Record]{
		Items:      []testkeyvalue.Versioned[testroutes.Record]{wantRoute},
		NextCursor: "next",
	}
	repository := &fakeRouteReadRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID}, Revision: 11, ReadRevision: 11,
		},
		route: wantRoute, page: want, wantRequest: request,
	}
	service, err := newRouteReadService(repository)
	if err != nil {
		t.Fatalf("newRouteReadService() error = %v", err)
	}
	page, err := service.ListRoutes(context.Background(), environmentID, request)
	if err != nil || !reflect.DeepEqual(page, want) || !repository.listed {
		t.Fatalf("ListRoutes() = %#v, %v, listed %t", page, err, repository.listed)
	}
	shown, err := service.GetRoute(context.Background(), routeID)
	if err != nil || !reflect.DeepEqual(shown, wantRoute) {
		t.Fatalf("GetRoute() = %#v, %v, want %#v", shown, err, wantRoute)
	}
}
