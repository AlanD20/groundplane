package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	corenetwork "github.com/AlanD20/groundplane/internal/core/network"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeRouteReader struct {
	route       corenetwork.Route
	page        corenetwork.Page[corenetwork.Route]
	wantRequest corenetwork.PageRequest
	listed      bool
}

func (fake *fakeRouteReader) GetRoute(
	context.Context, string,

) (corenetwork.Route, error) {
	return fake.route, nil
}

func (fake *fakeRouteReader) ListRoutes(
	_ context.Context,
	environmentID string,
	request corenetwork.PageRequest,
) (corenetwork.Page[corenetwork.Route], error) {
	fake.listed = environmentID == fake.route.EnvironmentID && request == fake.wantRequest
	return fake.page, nil
}

type fakeRouteMutator struct {
	createInput corenetwork.CreateRouteRequest
	editInput   corenetwork.EditRouteRequest
	routeID     string
	key         string
	response    corenetwork.MutationResponse
}

func (fake *fakeRouteMutator) CreateRoute(
	_ context.Context,
	input corenetwork.CreateRouteRequest,
	key string,
) (corenetwork.MutationResponse, error) {
	fake.createInput, fake.key = input, key
	return fake.response, nil
}

func (fake *fakeRouteMutator) EditRoute(
	_ context.Context,
	routeID string,
	input corenetwork.EditRouteRequest,
	key string,
) (corenetwork.MutationResponse, error) {
	fake.routeID, fake.editInput, fake.key = routeID, input, key
	return fake.response, nil
}

func (fake *fakeRouteMutator) RemoveRoute(
	_ context.Context,
	routeID string,
	key string,
) (corenetwork.MutationResponse, error) {
	fake.routeID, fake.key = routeID, key
	return fake.response, nil
}

func TestRouteHTTPBoundaryPreservesExactReadAndMutationContracts(t *testing.T) {
	// Rationale: Route list/show/create/edit are one operator capability across
	// Console, CLI, and API, so the HTTP boundary must preserve every field.
	t.Parallel()
	record := routeSurfaceTestRecord()
	request := corenetwork.PageRequest{Limit: 4, Cursor: "opaque"}
	reader := &fakeRouteReader{
		route: record,
		page: corenetwork.Page[corenetwork.Route]{
			Items: []corenetwork.Route{record}, NextCursor: "next",
		},
		wantRequest: request,
	}
	want := routeResponse(record)
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal Route: %v", err)
	}
	mutator := &fakeRouteMutator{response: corenetwork.MutationResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}}
	server := &Server{routeReads: reader, routeMutations: mutator}
	page, err := server.listRoutes(context.Background(), &routeListInput{
		Environment: record.EnvironmentID, Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil || !reader.listed || len(page.Body.Items) != 1 ||
		!reflect.DeepEqual(page.Body.Items[0], want) || page.Body.NextCursor != "next" {
		t.Fatalf("listRoutes() = %#v, %v, listed %t", page, err, reader.listed)
	}
	shown, err := server.showRoute(context.Background(), &routeShowInput{ID: record.ID})
	if err != nil || !reflect.DeepEqual(shown.Body, want) {
		t.Fatalf("showRoute() = %#v, %v", shown, err)
	}
	createInput := apiTypes.RouteCreate{
		EnvironmentID: record.EnvironmentID, Host: record.Host, Path: record.Path,
		Exposure: string(record.Exposure), TargetServiceID: record.TargetServiceID,
		TargetPort: record.TargetPort,
	}
	created, err := server.createRoute(context.Background(), &routeCreateInput{
		IdempotencyKey: "route-create-key-0001", Body: createInput,
	})
	wantCreate := corenetwork.CreateRouteRequest{
		EnvironmentID: createInput.EnvironmentID, Host: createInput.Host, Path: createInput.Path,
		Exposure:        corenetwork.RouteExposure(createInput.Exposure),
		TargetServiceID: createInput.TargetServiceID, TargetPort: createInput.TargetPort,
	}
	if err != nil || created.Status != http.StatusAccepted || !reflect.DeepEqual(mutator.createInput, wantCreate) ||
		mutator.key != "route-create-key-0001" {
		t.Fatalf("createRoute() = %#v, %v, forwarded %#v/%q", created, err, mutator.createInput, mutator.key)
	}
	mutator.response.Status = http.StatusAccepted
	edited, err := server.editRoute(context.Background(), &routeEditInput{
		ID: record.ID, IdempotencyKey: "route-edit-key-000001",
		Body: apiTypes.RouteEdit{Exposure: "internal"},
	})
	if err != nil || edited.Status != http.StatusAccepted || mutator.routeID != record.ID ||
		mutator.editInput.Exposure != "internal" || mutator.key != "route-edit-key-000001" {
		t.Fatalf(
			"editRoute() = %#v, %v, forwarded %q/%#v/%q",
			edited,
			err,
			mutator.routeID,
			mutator.editInput,
			mutator.key,
		)
	}
	mutator.response.Status = http.StatusAccepted
	removed, err := server.removeRoute(context.Background(), &routeRemoveInput{
		ID: record.ID, IdempotencyKey: "route-remove-key-0001",
	})
	if err != nil || removed.Status != http.StatusAccepted || mutator.routeID != record.ID ||
		mutator.key != "route-remove-key-0001" {
		t.Fatalf(
			"removeRoute() = %#v, %v, forwarded %q/%q",
			removed,
			err,
			mutator.routeID,
			mutator.key,
		)
	}
}

func routeSurfaceTestRecord() corenetwork.Route {
	at := time.Date(2026, time.August, 22, 20, 0, 0, 0, time.UTC)
	return corenetwork.Route{
		ID: ids.NewAt(ids.KindRoute, at, 2), EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 1),
		Host: "app.example.com", Path: "/api/*", Exposure: "public",
		TargetServiceID: ids.NewAt(ids.KindService, at, 3), TargetPort: 8080,
	}
}
