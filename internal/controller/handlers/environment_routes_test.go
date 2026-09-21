package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	environmentRouteProjectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentRouteID        = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentRouteTaskID    = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: Environment list and detail routes must translate the composed
// capability projection into the public API without persistence DTO leakage.
func TestEnvironmentReadRoutesExposeProvisioningProjection(t *testing.T) {
	t.Parallel()
	stub := &environmentRouteStub{environment: environmentForRouteTest(environmentcapability.Ready)}
	server := New(nil, nil, Options{Environments: environmentcapability.NewReader(stub)})

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

	stub.environment = environmentForRouteTest(environmentcapability.Failed)
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

// Rationale: rename input must remain strict while forwarding exactly one
// idempotency key to the Environment mutation capability.
func TestEnvironmentRenameRouteIsStrictAndForwardsIdempotency(t *testing.T) {
	t.Parallel()
	stub := &environmentRouteStub{renameResponse: testidempotency.IdempotencyResponse{
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

// Rationale: edit input must reject unknown, duplicate, absent, trailing, and
// unsupported-media bodies before invoking the Environment mutation.
func TestEnvironmentEditRouteIsStrictAndForwardsIdempotency(t *testing.T) {
	t.Parallel()
	stub := &environmentRouteStub{editResponse: testidempotency.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"network_pool":"10.40.0.0/15"}`),
	}}
	server := New(nil, nil, Options{EnvironmentChanges: stub})
	request := httptest.NewRequest(
		http.MethodPatch,
		"/api/v1/environments/"+environmentRouteID,
		strings.NewReader(`{"network_pool":"10.40.0.0/15"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "environment-edit-key-000001")
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || stub.editID != environmentRouteID ||
		stub.editInput.NetworkPool != "10.40.0.0/15" || stub.editKey != "environment-edit-key-000001" ||
		stub.editCalls != 1 {
		t.Fatalf(
			"edit response/call = %d, %q, %#v, %q",
			response.Code,
			stub.editID,
			stub.editInput,
			stub.editKey,
		)
	}
	body, _ := io.ReadAll(response.Result().Body)
	if string(body) != `{"network_pool":"10.40.0.0/15"}` {
		t.Fatalf("edit response body = %s", body)
	}

	invalidBodies := []string{
		`{"network_pool":"10.40.0.0/15","name":"live"}`,
		`{"network_pool":"10.40.0.0/15","network_pool":"10.42.0.0/15"}`,
		`{}`,
		`{"network_pool":"10.40.0.0/15"} {}`,
	}
	for index, body := range invalidBodies {
		invalid := httptest.NewRequest(
			http.MethodPatch,
			"/api/v1/environments/"+environmentRouteID,
			strings.NewReader(body),
		)
		invalid.Header.Set("Content-Type", "application/json")
		invalid.Header.Set(idempotencyKeyHeader, "environment-edit-invalid-000"+strconv.Itoa(index))
		invalidResponse := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(invalidResponse, invalid)
		if (invalidResponse.Code < 400 || invalidResponse.Code >= 500) || stub.editCalls != 1 {
			t.Fatalf("invalid body %q status = %d", body, invalidResponse.Code)
		}
	}

	octet := httptest.NewRequest(
		http.MethodPatch,
		"/api/v1/environments/"+environmentRouteID,
		strings.NewReader(`{"network_pool":"10.40.0.0/15"}`),
	)
	octet.Header.Set("Content-Type", "application/octet-stream")
	octet.Header.Set(idempotencyKeyHeader, "environment-edit-octet-0001")
	octetResponse := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(octetResponse, octet)
	if octetResponse.Code < 400 || octetResponse.Code >= 500 || stub.editCalls != 1 {
		t.Fatalf("octet-stream status = %d", octetResponse.Code)
	}
}

func environmentForRouteTest(state environmentcapability.ProvisioningState) environmentcapability.Environment {
	var createTaskID *string
	if state != environmentcapability.Ready {
		taskID := environmentRouteTaskID
		createTaskID = &taskID
	}
	return environmentcapability.Environment{NetworkPool: "10.40.0.0/16",
		ID: environmentRouteID, ProjectID: environmentRouteProjectID, Name: "production",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + environmentRouteProjectID + "/" + environmentRouteID,
		ProvisioningState: state, CreateTaskID: createTaskID,
	}
}

type environmentRouteStub struct {
	environment    environmentcapability.Environment
	listProjectID  string
	listRequest    environmentcapability.PageRequest
	renameID       string
	renameInput    environmentcapability.RenameEnvironmentInput
	renameKey      string
	renameResponse testidempotency.IdempotencyResponse
	editID         string
	editInput      environmentcapability.EditEnvironmentInput
	editKey        string
	editResponse   testidempotency.IdempotencyResponse
	editCalls      int
}

func (stub *environmentRouteStub) GetEnvironment(
	_ context.Context,
	_ string,
) (environmentcapability.Environment, error) {
	return stub.environment, nil
}

func (stub *environmentRouteStub) ListEnvironments(
	_ context.Context,
	projectID string,
	request environmentcapability.PageRequest,
) (environmentcapability.Page, error) {
	stub.listProjectID = projectID
	stub.listRequest = request
	return environmentcapability.Page{Items: []environmentcapability.Environment{stub.environment}}, nil
}

func (stub *environmentRouteStub) RenameEnvironment(
	_ context.Context,
	id string,
	input environmentcapability.RenameEnvironmentInput,
	key string,
) (testidempotency.IdempotencyResponse, error) {
	stub.renameID = id
	stub.renameInput = input
	stub.renameKey = key
	return stub.renameResponse, nil
}

func (stub *environmentRouteStub) EditEnvironment(
	_ context.Context,
	id string,
	input environmentcapability.EditEnvironmentInput,
	key string,
) (testidempotency.IdempotencyResponse, error) {
	stub.editCalls++
	stub.editID = id
	stub.editInput = input
	stub.editKey = key
	return stub.editResponse, nil
}
