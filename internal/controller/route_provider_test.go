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

// Rationale: a write between collection reads must not mix newer Service or
// Route state into the immutable provider input or fabricate a later revision.
func TestRouteProviderEnvironmentUsesOneFixedSnapshot(t *testing.T) {
	state := &concurrentRouteProviderState{snapshotRevision: 41}
	resolver := &TaskPlanResolver{routeState: state}
	environmentID := ids.New(ids.KindEnvironment)
	environment, revision, err := resolver.routeProviderEnvironment(
		context.Background(), environmentID, etcd.EnvironmentComposeProjection{}, nil, "",
	)
	if err != nil {
		t.Fatalf("routeProviderEnvironment() error = %v", err)
	}
	if revision != state.snapshotRevision || environment.Services["backend"].Image != "example/backend:old" ||
		len(environment.Routes) != 1 || environment.Routes[0].Host != "old.example.test" {
		t.Fatalf("fixed snapshot = revision %d, environment %#v", revision, environment)
	}
	if len(state.requested) != 3 {
		t.Fatalf("fixed snapshot requests = %#v", state.requested)
	}
	for _, requested := range state.requested {
		if requested != state.snapshotRevision {
			t.Fatalf("collection requested revision %d, want %d", requested, state.snapshotRevision)
		}
	}
}

// Rationale: labeling a mixed collection read with the selected snapshot is
// worse than failing planning because it creates false immutable evidence.
func TestRouteProviderEnvironmentRejectsRevisionDrift(t *testing.T) {
	state := &concurrentRouteProviderState{snapshotRevision: 41, driftCollection: "services"}
	resolver := &TaskPlanResolver{routeState: state}
	_, _, err := resolver.routeProviderEnvironment(
		context.Background(), ids.New(ids.KindEnvironment), etcd.EnvironmentComposeProjection{}, nil, "",
	)
	if err == nil {
		t.Fatal("routeProviderEnvironment() accepted a mixed-revision Service page")
	}
}
