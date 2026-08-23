package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeRouteReader struct {
	route       etcd.Versioned[etcd.RouteRecord]
	page        etcd.Page[etcd.RouteRecord]
	wantRequest etcd.PageRequest
	listed      bool
}

func (fake *fakeRouteReader) GetRoute(
	context.Context,
	string,
) (etcd.Versioned[etcd.RouteRecord], error) {
	return fake.route, nil
}

func (fake *fakeRouteReader) ListRoutes(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	fake.listed = environmentID == fake.route.Record.EnvironmentID && request == fake.wantRequest
	return fake.page, nil
}

type fakeRouteMutator struct {
	createInput apiTypes.RouteCreate
	editInput   apiTypes.RouteEdit
	routeID     string
	key         string
	response    etcd.IdempotencyResponse
}

func (fake *fakeRouteMutator) CreateRoute(
	_ context.Context,
	input apiTypes.RouteCreate,
	key string,
) (etcd.IdempotencyResponse, error) {
	fake.createInput, fake.key = input, key
	return fake.response, nil
}

func (fake *fakeRouteMutator) EditRoute(
	_ context.Context,
	routeID string,
	input apiTypes.RouteEdit,
	key string,
) (etcd.IdempotencyResponse, error) {
	fake.routeID, fake.editInput, fake.key = routeID, input, key
	return fake.response, nil
}

func (fake *fakeRouteMutator) RemoveRoute(
	_ context.Context,
	routeID string,
	key string,
) (etcd.IdempotencyResponse, error) {
	fake.routeID, fake.key = routeID, key
	return fake.response, nil
}

func TestRouteHTTPBoundaryPreservesExactReadAndMutationContracts(t *testing.T) {
	// Rationale: Route list/show/create/edit are one operator capability across
	// Console, CLI, and API, so the HTTP boundary must preserve every field.
	t.Parallel()
	record := routeSurfaceTestRecord()
	request := etcd.PageRequest{Limit: 4, Cursor: "opaque"}
	reader := &fakeRouteReader{
		route: etcd.Versioned[etcd.RouteRecord]{Record: record},
		page: etcd.Page[etcd.RouteRecord]{
			Items: []etcd.Versioned[etcd.RouteRecord]{{Record: record}}, NextCursor: "next",
		},
		wantRequest: request,
	}
	want := routeResponse(record)
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal Route: %v", err)
	}
	mutator := &fakeRouteMutator{response: etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: body,
	}}
	server := &Server{routeReads: reader, routeMutations: mutator}
	page, err := server.listRoutes(context.Background(), &routeListInput{
		Environment: record.EnvironmentID, Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil || !reader.listed || len(page.Body.Items) != 1 ||
		!reflect.DeepEqual(page.Body.Items[0], want) || page.Body.NextCursor != "next" {
		t.Fatalf("listRoutes() = %#v, %v, listed %t", page, err, reader.listed)
	}
	shown, err := server.showRoute(context.Background(), &routeShowInput{ID: record.Desired.ID})
	if err != nil || !reflect.DeepEqual(shown.Body, want) {
		t.Fatalf("showRoute() = %#v, %v", shown, err)
	}
	createInput := apiTypes.RouteCreate{
		EnvironmentID: record.EnvironmentID, Host: record.Desired.Host, Path: record.Desired.Path,
		Exposure: record.Desired.Exposure, TargetServiceID: record.Desired.TargetServiceID,
		TargetPort: record.Desired.TargetPort,
	}
	created, err := server.createRoute(context.Background(), &routeCreateInput{
		IdempotencyKey: "route-create-key-0001", Body: createInput,
	})
	if err != nil || created.Status != http.StatusCreated || !reflect.DeepEqual(mutator.createInput, createInput) ||
		mutator.key != "route-create-key-0001" {
		t.Fatalf("createRoute() = %#v, %v, forwarded %#v/%q", created, err, mutator.createInput, mutator.key)
	}
	mutator.response.Status = http.StatusOK
	edited, err := server.editRoute(context.Background(), &routeEditInput{
		ID: record.Desired.ID, IdempotencyKey: "route-edit-key-000001",
		Body: apiTypes.RouteEdit{Exposure: "internal"},
	})
	if err != nil || edited.Status != http.StatusOK || mutator.routeID != record.Desired.ID ||
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
		ID: record.Desired.ID, IdempotencyKey: "route-remove-key-0001",
	})
	if err != nil || removed.Status != http.StatusAccepted || mutator.routeID != record.Desired.ID ||
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

func TestRouteOpenAPIContainsCanonicalOperations(t *testing.T) {
	// Rationale: generated Console and CLI clients depend on canonical Route
	// operation identities rather than handwritten transport paths.
	t.Parallel()
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	var contract struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(document, &contract); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	want := []struct {
		method      string
		path        string
		operationID string
	}{
		{method: "get", path: "/routes", operationID: "route.list"},
		{method: "post", path: "/routes", operationID: "route.create"},
		{method: "get", path: "/routes/{id}", operationID: "route.show"},
		{method: "patch", path: "/routes/{id}", operationID: "route.edit"},
		{method: "delete", path: "/routes/{id}", operationID: "route.remove"},
	}
	for _, operation := range want {
		if got := contract.Paths[operation.path][operation.method].OperationID; got != operation.operationID {
			t.Fatalf("%s %s operationId = %q, want %q", operation.method, operation.path, got, operation.operationID)
		}
	}
}

func routeSurfaceTestRecord() etcd.RouteRecord {
	at := time.Date(2026, time.August, 22, 20, 0, 0, 0, time.UTC)
	return etcd.RouteRecord{
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 1),
		Desired: core.Route{
			ID: ids.NewAt(ids.KindRoute, at, 2), Host: "app.example.com", Path: "/api/*",
			Exposure: "public", TargetServiceID: ids.NewAt(ids.KindService, at, 3), TargetPort: 8080,
		},
	}
}
