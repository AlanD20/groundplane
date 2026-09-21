package taskplanning

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
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
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testzones.Record], error) {
	state.requested = append(state.requested, request.Revision)
	state.advanced = true
	return testkeyvalue.Page[testzones.Record]{
		Revision: state.collectionRevision("zones"),
		Items: []testkeyvalue.Versioned[testzones.Record]{{Record: testzones.Record{
			EnvironmentID: environmentID,
			Desired:       core.Zone{ID: ids.New(ids.KindNetwork), Name: "frontend"},
		}}},
	}, nil
}

func (state *concurrentRouteProviderState) ListServices(
	_ context.Context,
	environmentID string,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testservices.ServiceRecord], error) {
	state.requested = append(state.requested, request.Revision)
	image := "example/backend:old"
	if request.Revision != state.snapshotRevision && state.advanced {
		image = "example/backend:new"
	}
	return testkeyvalue.Page[testservices.ServiceRecord]{
		Revision: state.collectionRevision("services"),
		Items: []testkeyvalue.Versioned[testservices.ServiceRecord]{{Record: testservices.ServiceRecord{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: ids.New(ids.KindService), Name: "backend", Image: image},
		}}},
	}, nil
}

func (state *concurrentRouteProviderState) ListRoutes(
	_ context.Context,
	environmentID string,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testroutes.Record], error) {
	state.requested = append(state.requested, request.Revision)
	host := "old.example.test"
	if request.Revision != state.snapshotRevision && state.advanced {
		host = "new.example.test"
	}
	return testkeyvalue.Page[testroutes.Record]{
		Revision: state.collectionRevision("routes"),
		Items: []testkeyvalue.Versioned[testroutes.Record]{{Record: testroutes.Record{
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
) testenvironmentprojection.EnvironmentComposeProjection {
	serviceID := ids.New(ids.KindService)
	routeID := ids.New(ids.KindRoute)
	return testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: serviceID, Name: "backend", Image: serviceImage},
		}},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{{
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
		&testroutes.Record{EnvironmentID: environmentID, Desired: desired}, "",
	)
	if err != nil {
		t.Fatalf("routeProviderEnvironment() error = %v", err)
	}
	if revision != state.snapshotRevision || len(environment.Routes) != 1 ||
		environment.Routes[0].Host != "new.example.test" {
		t.Fatalf("selected route head = revision %d, routes %#v", revision, environment.Routes)
	}
}
