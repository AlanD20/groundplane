package controller

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
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const testProjectRouteID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestProjectCreateRoutePreservesExactMutationResponse(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body:        []byte(`{"id":"` + testProjectRouteID + `"}`),
	}
	mutator := &fakeProjectMutator{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ProjectMutations: mutator})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/projects", bytes.NewBufferString(
		`{"tenant_id":"`+testTenantRouteID+`","slug":"console","name":"Console","description":"Operator interface"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "project-create-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) {
		t.Fatalf(
			"POST /projects = %d %q %s",
			response.Code,
			response.Header().Get("Content-Type"),
			response.Body.Bytes(),
		)
	}
	if mutator.input.TenantID != testTenantRouteID || mutator.input.Slug != "console" ||
		mutator.input.Name == nil || *mutator.input.Name != "Console" ||
		mutator.input.Description != "Operator interface" || mutator.key != "project-create-key-0001" {
		t.Fatalf("CreateProject() input/key = %#v/%q", mutator.input, mutator.key)
	}
}

func TestProjectCreateRouteRejectsInvalidBodies(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		body   []byte
		status int
	}{
		{name: "duplicate", body: []byte(`{"tenant_id":"` + testTenantRouteID + `","slug":"a","slug":"b"}`), status: http.StatusBadRequest},
		{name: "unknown", body: []byte(`{"tenant_id":"` + testTenantRouteID + `","slug":"a","kind":"backing"}`), status: http.StatusBadRequest},
		{name: "non-string", body: []byte(`{"tenant_id":"` + testTenantRouteID + `","slug":42}`), status: http.StatusUnprocessableEntity},
		{name: "not-object", body: []byte(`[]`), status: http.StatusBadRequest},
		{name: "trailing", body: []byte(`{"tenant_id":"` + testTenantRouteID + `","slug":"a"}{}`), status: http.StatusBadRequest},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mutator := &fakeProjectMutator{}
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ProjectMutations: mutator})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/projects", bytes.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(idempotencyKeyHeader, "project-create-key-0003")
			response := httptest.NewRecorder()
			server.Mux.ServeHTTP(response, request)
			if response.Code != test.status || mutator.called {
				t.Fatalf("status/called = %d/%t, body = %s", response.Code, mutator.called, response.Body.String())
			}
		})
	}
}

func TestProjectReadRoutesPreserveKindFiltersAndProjection(t *testing.T) {
	t.Parallel()
	record := core.Project{
		ID: testProjectRouteID, TenantID: testTenantRouteID, Slug: "console",
		Name: "Console", Description: "Operator interface", Kind: core.ProjectKindTenant,
	}
	reader := &fakeProjectReader{record: record}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Projects: reader})
	listRequest := httptest.NewRequest(
		http.MethodGet, "/api/v1/projects?kind=tenant&tenant="+testTenantRouteID+"&limit=1&cursor=opaque", nil,
	)
	listResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(listResponse, listRequest)
	var page apiTypes.Page[apiTypes.Project]
	if listResponse.Code != http.StatusOK || json.Unmarshal(listResponse.Body.Bytes(), &page) != nil ||
		len(page.Items) != 1 || page.Items[0] != projectAPI(record) || page.NextCursor != "next" {
		t.Fatalf("GET /projects = %d %#v", listResponse.Code, page)
	}
	if reader.filter != (hierarchy.ProjectFilter{TenantID: testTenantRouteID, Kind: core.ProjectKindTenant}) ||
		reader.page != (hierarchy.PageRequest{Limit: 1, Cursor: "opaque"}) {
		t.Fatalf("ListAllProjects() filter/page = %#v/%#v", reader.filter, reader.page)
	}
	showResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(showResponse, httptest.NewRequest(http.MethodGet, "/api/v1/projects/"+testProjectRouteID, nil))
	if showResponse.Code != http.StatusOK || reader.id != testProjectRouteID {
		t.Fatalf("GET /projects/id = %d, id %q", showResponse.Code, reader.id)
	}
}

type fakeProjectMutator struct {
	input    hierarchy.CreateProjectInput
	key      string
	response etcd.IdempotencyResponse
	called   bool
}

func (mutator *fakeProjectMutator) CreateProject(
	_ context.Context,
	input hierarchy.CreateProjectInput,
	key string,
) (etcd.IdempotencyResponse, error) {
	mutator.called = true
	mutator.input = input
	mutator.key = key
	return mutator.response, nil
}

type fakeProjectReader struct {
	record core.Project
	id     string
	filter hierarchy.ProjectFilter
	page   hierarchy.PageRequest
}

func (reader *fakeProjectReader) GetProject(
	_ context.Context,
	id string,
) (hierarchy.Versioned[core.Project], error) {
	reader.id = id
	return hierarchy.Versioned[core.Project]{Record: reader.record, Revision: 1, ReadRevision: 1}, nil
}

func (reader *fakeProjectReader) ListAllProjects(
	_ context.Context,
	filter hierarchy.ProjectFilter,
	page hierarchy.PageRequest,
) (hierarchy.Page[core.Project], error) {
	reader.filter = filter
	reader.page = page
	return hierarchy.Page[core.Project]{
		Items:      []hierarchy.Versioned[core.Project]{{Record: reader.record, Revision: 1, ReadRevision: 1}},
		NextCursor: "next", Revision: 1,
	}, nil
}
