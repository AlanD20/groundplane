package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	environmentRouteProjectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentRouteID        = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentRouteTaskID    = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestEnvironmentReadRoutesExposeProvisioningProjection(t *testing.T) {
	t.Parallel()
	stub := &environmentRouteStub{record: EnvironmentRecordForRouteTest(etcd.EnvironmentProvisioningReady)}
	server := New(nil, nil, Options{Environments: stub})

	listRequest := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/environments?project="+environmentRouteProjectID+"&limit=2",
		nil,
	)
	listResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || stub.listProjectID != environmentRouteProjectID ||
		stub.listRequest.Limit != 2 {
		t.Fatalf("list response/scope = %d, %q, %#v", listResponse.Code, stub.listProjectID, stub.listRequest)
	}
	var page apiTypes.Page[apiTypes.Environment]
	if err := json.Unmarshal(listResponse.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].CreateTaskID != nil ||
		page.Items[0].ProvisioningState != apiTypes.EnvironmentReady {
		t.Fatalf("ready Environment projection = %#v", page.Items)
	}

	stub.record = EnvironmentRecordForRouteTest(etcd.EnvironmentProvisioningFailed)
	showRequest := httptest.NewRequest(http.MethodGet, "/api/v1/environments/"+environmentRouteID, nil)
	showResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(showResponse, showRequest)
	var detail apiTypes.Environment
	if err := json.Unmarshal(showResponse.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail response: %v", err)
	}
	if showResponse.Code != http.StatusOK || detail.CreateTaskID == nil ||
		*detail.CreateTaskID != environmentRouteTaskID || detail.ProvisioningState != apiTypes.EnvironmentFailed {
		t.Fatalf("failed Environment projection = %#v, status %d", detail, showResponse.Code)
	}
}

func TestEnvironmentRenameRouteIsStrictAndForwardsIdempotency(t *testing.T) {
	t.Parallel()
	stub := &environmentRouteStub{renameResponse: etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"name":"live"}`),
	}}
	server := New(nil, nil, Options{EnvironmentChanges: stub})
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/environments/"+environmentRouteID+"/rename",
		strings.NewReader(`{"name":"live"}`),
	)
	request.Header.Set(idempotencyKeyHeader, "environment-rename-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK || stub.renameID != environmentRouteID ||
		stub.renameInput.Name != "live" || stub.renameKey != "environment-rename-key-0001" {
		t.Fatalf(
			"rename response/call = %d, %q, %#v, %q",
			response.Code,
			stub.renameID,
			stub.renameInput,
			stub.renameKey,
		)
	}
	body, _ := io.ReadAll(response.Result().Body)
	if string(body) != `{"name":"live"}` {
		t.Fatalf("rename response body = %s", body)
	}

	duplicate := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/environments/"+environmentRouteID+"/rename",
		strings.NewReader(`{"name":"a","name":"b"}`),
	)
	duplicate.Header.Set(idempotencyKeyHeader, "environment-rename-key-0002")
	duplicateResponse := httptest.NewRecorder()
	server.Mux.ServeHTTP(duplicateResponse, duplicate)
	if duplicateResponse.Code != http.StatusBadRequest {
		t.Fatalf("duplicate member status = %d", duplicateResponse.Code)
	}
}

func EnvironmentRecordForRouteTest(state etcd.EnvironmentProvisioningState) etcd.EnvironmentRecord {
	return etcd.EnvironmentRecord{
		ID: environmentRouteID, ProjectID: environmentRouteProjectID, Name: "production",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + environmentRouteProjectID + "/" + environmentRouteID,
		ProvisioningState: state, CreateTaskID: environmentRouteTaskID,
	}
}

type environmentRouteStub struct {
	record         etcd.EnvironmentRecord
	listProjectID  string
	listRequest    etcd.PageRequest
	renameID       string
	renameInput    hierarchy.RenameEnvironmentInput
	renameKey      string
	renameResponse etcd.IdempotencyResponse
}

func (stub *environmentRouteStub) GetEnvironment(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return etcd.Versioned[etcd.EnvironmentRecord]{Record: stub.record, Revision: 1, ReadRevision: 1}, nil
}

func (stub *environmentRouteStub) ListEnvironments(
	_ context.Context,
	projectID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.EnvironmentRecord], error) {
	stub.listProjectID = projectID
	stub.listRequest = request
	return etcd.Page[etcd.EnvironmentRecord]{
		Items:    []etcd.Versioned[etcd.EnvironmentRecord]{{Record: stub.record, Revision: 1, ReadRevision: 1}},
		Revision: 1,
	}, nil
}

func (stub *environmentRouteStub) RenameEnvironment(
	_ context.Context,
	id string,
	input hierarchy.RenameEnvironmentInput,
	key string,
) (etcd.IdempotencyResponse, error) {
	stub.renameID = id
	stub.renameInput = input
	stub.renameKey = key
	return stub.renameResponse, nil
}
