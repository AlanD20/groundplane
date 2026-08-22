package controller

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestProjectChangeRoutesPreserveExactResponses(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"` + testProjectRouteID + `"}`),
	}
	changer := &fakeProjectChanger{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ProjectChanges: changer})
	edit := httptest.NewRequest(http.MethodPatch, "/api/v1/projects/"+testProjectRouteID,
		bytes.NewBufferString(`{"name":"Operator Console"}`))
	edit.Header.Set("Content-Type", "application/json")
	edit.Header.Set(idempotencyKeyHeader, "project-edit-key-0001")
	editResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(editResponse, edit)
	if editResponse.Code != http.StatusOK || !bytes.Equal(editResponse.Body.Bytes(), want.Body) ||
		changer.edit.Name == nil || *changer.edit.Name != "Operator Console" || changer.id != testProjectRouteID {
		t.Fatalf(
			"PATCH /projects response/input = %d %s %#v",
			editResponse.Code,
			editResponse.Body.Bytes(),
			changer.edit,
		)
	}
	rename := httptest.NewRequest(http.MethodPost, "/api/v1/projects/"+testProjectRouteID+"/rename",
		bytes.NewBufferString(`{"slug":"operator-console"}`))
	rename.Header.Set("Content-Type", "application/json")
	rename.Header.Set(idempotencyKeyHeader, "project-rename-key-0001")
	renameResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(renameResponse, rename)
	if renameResponse.Code != http.StatusOK || changer.rename.Slug != "operator-console" {
		t.Fatalf("POST /projects/id/rename = %d %#v", renameResponse.Code, changer.rename)
	}
}

func TestProjectChangeRoutesRejectDescriptionEmptyDuplicateAndUnknownBodies(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		method string
		path   string
		body   string
		status int
	}{
		{method: http.MethodPatch, path: "/api/v1/projects/" + testProjectRouteID, body: `{}`, status: http.StatusUnprocessableEntity},
		{method: http.MethodPatch, path: "/api/v1/projects/" + testProjectRouteID, body: `{"description":"no"}`, status: http.StatusBadRequest},
		{method: http.MethodPatch, path: "/api/v1/projects/" + testProjectRouteID, body: `{"name":"A","name":"B"}`, status: http.StatusBadRequest},
		{method: http.MethodPost, path: "/api/v1/projects/" + testProjectRouteID + "/rename", body: `{}`, status: http.StatusUnprocessableEntity},
	} {
		changer := &fakeProjectChanger{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ProjectChanges: changer})
		request := httptest.NewRequest(test.method, test.path, bytes.NewBufferString(test.body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "project-change-key-0003")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != test.status || changer.called {
			t.Fatalf("%s %s body %s = %d, called %t", test.method, test.path, test.body, response.Code, changer.called)
		}
	}
}

type fakeProjectChanger struct {
	id       string
	edit     hierarchy.EditProjectInput
	rename   hierarchy.RenameProjectInput
	response etcd.IdempotencyResponse
	called   bool
}

func (changer *fakeProjectChanger) EditProject(
	_ context.Context,
	id string,
	input hierarchy.EditProjectInput,
	_ string,
) (etcd.IdempotencyResponse, error) {
	changer.called = true
	changer.id = id
	changer.edit = input
	return changer.response, nil
}

func (changer *fakeProjectChanger) RenameProject(
	_ context.Context,
	id string,
	input hierarchy.RenameProjectInput,
	_ string,
) (etcd.IdempotencyResponse, error) {
	changer.called = true
	changer.id = id
	changer.rename = input
	return changer.response, nil
}
