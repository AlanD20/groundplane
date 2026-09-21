package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/core"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const testTenantRouteID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestTenantCreateRoutePreservesExactMutationResponse(t *testing.T) {
	t.Parallel()

	want := testidempotency.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: []byte(`{"id":"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV","slug":"acme","name":"Acme","description":"Production"}`),
	}
	mutator := &fakeTenantMutator{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{TenantMutations: mutator})
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tenants",
		bytes.NewBufferString(`{"slug":"acme","name":"Acme","description":"Production"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "tenant-create-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) {
		t.Fatalf(
			"POST /tenants = %d %q %s",
			response.Code,
			response.Header().Get("Content-Type"),
			response.Body.Bytes(),
		)
	}
	if mutator.input.Slug != "acme" || mutator.input.Name == nil || *mutator.input.Name != "Acme" ||
		mutator.input.Description != "Production" || mutator.key != "tenant-create-key-0001" {
		t.Fatalf("CreateTenant() input/key = %#v/%q", mutator.input, mutator.key)
	}
}

func TestTenantCreateRouteRejectsInvalidBodies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   []byte
		status int
	}{
		{name: "duplicate", body: []byte(`{"slug":"acme","slug":"other"}`), status: http.StatusBadRequest},
		{name: "unknown", body: []byte(`{"slug":"acme","owner":"platform"}`), status: http.StatusBadRequest},
		{name: "unknown non-string", body: []byte(`{"slug":"acme","owner":1}`), status: http.StatusBadRequest},
		{
			name:   "invalid utf8",
			body:   []byte{'{', '"', 's', 'l', 'u', 'g', '"', ':', '"', 0xff, '"', '}'},
			status: http.StatusBadRequest,
		},
		{name: "non-string", body: []byte(`{"slug":42}`), status: http.StatusUnprocessableEntity},
		{name: "not object", body: []byte(`[]`), status: http.StatusBadRequest},
		{name: "trailing", body: []byte(`{"slug":"acme"}{}`), status: http.StatusBadRequest},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mutator := &fakeTenantMutator{}
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{TenantMutations: mutator})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", bytes.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(idempotencyKeyHeader, "tenant-create-key-0003")
			response := httptest.NewRecorder()
			server.Mux.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if mutator.called {
				t.Fatal("CreateTenant() called for rejected body")
			}
		})
	}
}

func TestTenantChangeRoutesPreserveExactResponses(t *testing.T) {
	t.Parallel()

	want := testidempotency.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json",
		Body: []byte(`{"id":"` + testTenantRouteID + `","slug":"acme","name":"Acme","description":"Updated"}`),
	}
	changer := &fakeTenantChanger{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{TenantChanges: changer})

	edit := httptest.NewRequest(
		http.MethodPatch,
		"/api/v1/tenants/"+testTenantRouteID,
		bytes.NewBufferString(`{"description":"Updated"}`),
	)
	edit.Header.Set("Content-Type", "application/json")
	edit.Header.Set(idempotencyKeyHeader, "tenant-edit-key-0001")
	editResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(editResponse, edit)
	if editResponse.Code != http.StatusOK || !bytes.Equal(editResponse.Body.Bytes(), want.Body) ||
		changer.edit.Description == nil || *changer.edit.Description != "Updated" || changer.id != testTenantRouteID {
		t.Fatalf(
			"PATCH /tenants response/input = %d %s %#v",
			editResponse.Code,
			editResponse.Body.Bytes(),
			changer.edit,
		)
	}

	rename := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tenants/"+testTenantRouteID+"/rename",
		bytes.NewBufferString(`{"slug":"acme-inc"}`),
	)
	rename.Header.Set("Content-Type", "application/json")
	rename.Header.Set(idempotencyKeyHeader, "tenant-rename-key-0001")
	renameResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(renameResponse, rename)
	if renameResponse.Code != http.StatusOK || changer.rename.Slug != "acme-inc" {
		t.Fatalf("POST /tenants/id/rename = %d %#v", renameResponse.Code, changer.rename)
	}
}

func TestTenantChangeRoutesRejectEmptyDuplicateAndUnknownBodies(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		method string
		path   string
		body   string
		status int
	}{
		{method: http.MethodPatch, path: "/api/v1/tenants/" + testTenantRouteID, body: `{}`, status: http.StatusUnprocessableEntity},
		{method: http.MethodPatch, path: "/api/v1/tenants/" + testTenantRouteID, body: `{"name":"A","name":"B"}`, status: http.StatusBadRequest},
		{method: http.MethodPatch, path: "/api/v1/tenants/" + testTenantRouteID, body: `{"slug":"other"}`, status: http.StatusBadRequest},
		{method: http.MethodPost, path: "/api/v1/tenants/" + testTenantRouteID + "/rename", body: `{}`, status: http.StatusUnprocessableEntity},
	} {
		changer := &fakeTenantChanger{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{TenantChanges: changer})
		request := httptest.NewRequest(test.method, test.path, bytes.NewBufferString(test.body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "tenant-change-key-0003")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != test.status || changer.called {
			t.Fatalf("%s %s body %s = %d, called %t", test.method, test.path, test.body, response.Code, changer.called)
		}
	}
}

func TestTenantReadRoutesProjectDescriptionAndCursor(t *testing.T) {
	t.Parallel()

	record := core.Tenant{
		ID: testTenantRouteID, Slug: "acme", Name: "Acme", Description: "Production workloads",
	}
	reader := &fakeTenantReader{record: record}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Tenants: reader})

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/tenants?limit=1&cursor=opaque", nil)
	listResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listResponse.Code, listResponse.Body.String())
	}
	var page apiTypes.Page[apiTypes.Tenant]
	if err := json.Unmarshal(listResponse.Body.Bytes(), &page); err != nil || len(page.Items) != 1 ||
		page.Items[0].Description != record.Description || page.NextCursor != "next" {
		t.Fatalf("Tenant page = %#v, %v", page, err)
	}
	if reader.pageRequest != (hierarchy.PageRequest{Limit: 1, Cursor: "opaque"}) {
		t.Fatalf("ListTenants() request = %#v", reader.pageRequest)
	}

	showRequest := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/"+testTenantRouteID, nil)
	showResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(showResponse, showRequest)
	if showResponse.Code != http.StatusOK {
		t.Fatalf("show status = %d, body = %s", showResponse.Code, showResponse.Body.String())
	}
	var tenant apiTypes.Tenant
	if err := json.Unmarshal(showResponse.Body.Bytes(), &tenant); err != nil || tenant != tenantAPI(record) {
		t.Fatalf("Tenant detail = %#v, %v", tenant, err)
	}
	if reader.id != testTenantRouteID {
		t.Fatalf("GetTenant() id = %q", reader.id)
	}
}

func TestTenantListRejectsUnknownAndRepeatedQuery(t *testing.T) {
	t.Parallel()

	for _, query := range []string{"?status=active", "?limit=1&limit=2", "?cursor=a&cursor=b"} {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/tenants"+query, nil)
		response := httptest.NewRecorder()
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Tenants: &fakeTenantReader{}})
		server.Mux.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, body = %s", query, response.Code, response.Body.String())
		}
	}
}

type fakeTenantReader struct {
	record      core.Tenant
	id          string
	pageRequest hierarchy.PageRequest
}

type fakeTenantMutator struct {
	input    hierarchy.CreateTenantInput
	key      string
	response testidempotency.IdempotencyResponse
	called   bool
}

type fakeTenantChanger struct {
	id       string
	edit     hierarchy.EditTenantInput
	rename   hierarchy.RenameTenantInput
	response testidempotency.IdempotencyResponse
	called   bool
}

func (changer *fakeTenantChanger) EditTenant(
	_ context.Context,
	id string,
	input hierarchy.EditTenantInput,
	_ string,
) (testidempotency.IdempotencyResponse, error) {
	changer.called = true
	changer.id = id
	changer.edit = input
	return changer.response, nil
}

func (changer *fakeTenantChanger) RenameTenant(
	_ context.Context,
	id string,
	input hierarchy.RenameTenantInput,
	_ string,
) (testidempotency.IdempotencyResponse, error) {
	changer.called = true
	changer.id = id
	changer.rename = input
	return changer.response, nil
}

func (mutator *fakeTenantMutator) CreateTenant(
	_ context.Context,
	input hierarchy.CreateTenantInput,
	key string,
) (testidempotency.IdempotencyResponse, error) {
	mutator.called = true
	mutator.input = input
	mutator.key = key
	return mutator.response, nil
}

func (reader *fakeTenantReader) GetTenant(
	_ context.Context,
	id string,
) (hierarchy.Versioned[core.Tenant], error) {
	reader.id = id
	return hierarchy.Versioned[core.Tenant]{Record: reader.record, Revision: 1, ReadRevision: 1}, nil
}

func (reader *fakeTenantReader) ListTenants(
	_ context.Context,
	request hierarchy.PageRequest,
) (hierarchy.Page[core.Tenant], error) {
	reader.pageRequest = request
	return hierarchy.Page[core.Tenant]{
		Items: []hierarchy.Versioned[core.Tenant]{
			{Record: reader.record, Revision: 1, ReadRevision: 1},
		},
		NextCursor: "next", Revision: 1,
	}, nil
}
