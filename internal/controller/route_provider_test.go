package controller

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type concurrentRouteProviderState struct {
	snapshotRevision int64
	driftCollection  string
	requested        []int64
	advanced         bool
}

func (state *concurrentRouteProviderState) SnapshotRevision(context.Context) (int64, error) {
	return state.snapshotRevision, nil
}

func (state *concurrentRouteProviderState) ListZones(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	state.requested = append(state.requested, request.Revision)
	state.advanced = true
	return etcd.Page[etcd.ZoneRecord]{
		Revision: state.collectionRevision("zones"),
		Items: []etcd.Versioned[etcd.ZoneRecord]{{Record: etcd.ZoneRecord{
			EnvironmentID: environmentID,
			Desired:       core.Zone{ID: ids.New(ids.KindNetwork), Name: "frontend"},
		}}},
	}, nil
}

func (state *concurrentRouteProviderState) ListServices(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	state.requested = append(state.requested, request.Revision)
	image := "example/backend:old"
	if request.Revision != state.snapshotRevision && state.advanced {
		image = "example/backend:new"
	}
	return etcd.Page[etcd.ServiceRecord]{
		Revision: state.collectionRevision("services"),
		Items: []etcd.Versioned[etcd.ServiceRecord]{{Record: etcd.ServiceRecord{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: ids.New(ids.KindService), Name: "backend", Image: image},
		}}},
	}, nil
}

func (state *concurrentRouteProviderState) ListRoutes(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	state.requested = append(state.requested, request.Revision)
	host := "old.example.test"
	if request.Revision != state.snapshotRevision && state.advanced {
		host = "new.example.test"
	}
	return etcd.Page[etcd.RouteRecord]{
		Revision: state.collectionRevision("routes"),
		Items: []etcd.Versioned[etcd.RouteRecord]{{Record: etcd.RouteRecord{
			EnvironmentID: environmentID,
			Desired: core.Route{
				ID: ids.New(ids.KindRoute), Host: host, Path: "/", Exposure: "public",
				TargetServiceID: ids.New(ids.KindService), TargetPort: 8080,
			},
		}}},
	}, nil
}

func (state *concurrentRouteProviderState) collectionRevision(collection string) int64 {
	if state.driftCollection == collection {
		return state.snapshotRevision + 1
	}
	return state.snapshotRevision
}

func routeProviderEnvironmentProjection(
	environmentID, serviceImage, routeHost string,
) etcd.EnvironmentComposeProjection {
	serviceID := ids.New(ids.KindService)
	routeID := ids.New(ids.KindRoute)
	return etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID,
		DesiredServices: []etcd.EnvironmentServiceProjection{{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: serviceID, Name: "backend", Image: serviceImage},
		}},
		DesiredRoutes: []etcd.EnvironmentRouteProjection{{
			EnvironmentID: environmentID,
			Desired: core.Route{
				ID: routeID, Host: routeHost, Path: "/", Exposure: "public",
				TargetServiceID: serviceID, TargetPort: 8080,
			},
			DesiredGeneration: 1,
		}},
	}
}

// Rationale: the provider input is a selected Environment desired projection
// and must retain its fixed Environment revision and Route head.
func TestRouteProviderEnvironmentUsesOneFixedSnapshot(t *testing.T) {
	state := &concurrentRouteProviderState{snapshotRevision: 41}
	resolver := &TaskPlanResolver{routeState: state}
	environmentID := ids.New(ids.KindEnvironment)
	projection := routeProviderEnvironmentProjection(environmentID, "example/backend:old", "old.example.test")
	environment, revision, err := resolver.routeProviderEnvironment(
		context.Background(), environmentID, state.snapshotRevision, projection, nil, "",
	)
	if err != nil {
		t.Fatalf("routeProviderEnvironment() error = %v", err)
	}
	if revision != state.snapshotRevision || environment.Services["backend"].Image != "example/backend:old" ||
		len(environment.Routes) != 1 || environment.Routes[0].Host != "old.example.test" {
		t.Fatalf("fixed snapshot = revision %d, environment %#v", revision, environment)
	}
}

// Rationale: a pending Route head must replace the stale Route selected by the
// Environment projection without changing the fixed Environment revision.
func TestRouteProviderEnvironmentUsesSelectedRouteHead(t *testing.T) {
	state := &concurrentRouteProviderState{snapshotRevision: 41}
	resolver := &TaskPlanResolver{routeState: state}
	environmentID := ids.New(ids.KindEnvironment)
	projection := routeProviderEnvironmentProjection(environmentID, "example/backend:old", "old.example.test")
	desired := projection.DesiredRoutes[0].Desired
	desired.Host = "new.example.test"
	environment, revision, err := resolver.routeProviderEnvironment(
		context.Background(), environmentID, state.snapshotRevision, projection,
		&etcd.RouteRecord{EnvironmentID: environmentID, Desired: desired}, "",
	)
	if err != nil {
		t.Fatalf("routeProviderEnvironment() error = %v", err)
	}
	if revision != state.snapshotRevision || len(environment.Routes) != 1 ||
		environment.Routes[0].Host != "new.example.test" {
		t.Fatalf("selected route head = revision %d, routes %#v", revision, environment.Routes)
	}
}
