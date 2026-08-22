package app

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeRouteReadRepository struct {
	routeReadRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	route       etcd.Versioned[etcd.RouteRecord]
	page        etcd.Page[etcd.RouteRecord]
	wantRequest etcd.PageRequest
	listed      bool
}

func (fake *fakeRouteReadRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeRouteReadRepository) GetRoute(
	context.Context,
	string,
) (etcd.Versioned[etcd.RouteRecord], error) {
	return fake.route, nil
}

func (fake *fakeRouteReadRepository) ListRoutes(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

func TestRouteReadsVerifyOwnerAndPreserveDurableResults(t *testing.T) {
	// Rationale: Route collections are Environment-scoped while detail reads
	// preserve the exact stable target and match stored by the Controller.
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	routeID := "rte_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := etcd.PageRequest{Limit: 17, Cursor: "opaque"}
	wantRoute := etcd.Versioned[etcd.RouteRecord]{
		Record: etcd.RouteRecord{EnvironmentID: environmentID}, Revision: 19, ReadRevision: 19,
	}
	wantRoute.Record.Desired.ID = routeID
	want := etcd.Page[etcd.RouteRecord]{Items: []etcd.Versioned[etcd.RouteRecord]{wantRoute}, NextCursor: "next"}
	repository := &fakeRouteReadRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{ID: environmentID}, Revision: 11, ReadRevision: 11,
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
