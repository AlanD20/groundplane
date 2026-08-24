package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: GET absence and PUT full replacement are the complete public
// configuration vertical and must share the exact stable response contract.
func TestBackupPolicyRoutesExposeEffectiveDisabledAndProtectedReplacement(t *testing.T) {
	service := &backupPolicyRouteTestService{policy: apiTypes.BackupPolicy{Sources: []apiTypes.BackupSource{}}}
	server := New(nil, nil, Options{BackupPolicies: service, BackupPolicyMutations: service})
	environmentID := "env_01AAAAAAAAAAAAAAAAAAAAAAAA"
	get := httptest.NewRequest(http.MethodGet, "/api/v1/environments/"+environmentID+"/backup-policy", nil)
	got := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(got, get)
	var shown apiTypes.BackupPolicy
	if err := json.Unmarshal(got.Body.Bytes(), &shown); err != nil {
		t.Fatal(err)
	}
	if got.Code != http.StatusOK || shown.Enabled || shown.Sources == nil || len(shown.Sources) != 0 {
		t.Fatalf("GET status/body = %d, %q", got.Code, got.Body.String())
	}
	body := `{"enabled":true,"frequency":"*-*-* 03:15:00","keep":2,"encryption":"age",` +
		`"connector_id":"con_01AAAAAAAAAAAAAAAAAAAAAAAA","sources":[` +
		`{"kind":"volume","target_id":"vol_01AAAAAAAAAAAAAAAAAAAAAAAA"}]}`
	put := httptest.NewRequest(
		http.MethodPut, "/api/v1/environments/"+environmentID+"/backup-policy", strings.NewReader(body),
	)
	put.Header.Set("Content-Type", "application/json")
	put.Header.Set("Idempotency-Key", "0123456789abcdef")
	replaced := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(replaced, put)
	if replaced.Code != http.StatusOK || service.setCalls != 1 {
		t.Fatalf("PUT status/calls/body = %d, %d, %q", replaced.Code, service.setCalls, replaced.Body.String())
	}
	if len(service.input.Sources) != 1 || service.input.Sources[0].TargetID != "vol_01AAAAAAAAAAAAAAAAAAAAAAAA" {
		t.Fatalf("PUT input = %#v", service.input)
	}
}

// Rationale: malformed JSON and the 12-source operation budget must fail at
// the public boundary before the mutation service is invoked.
func TestBackupPolicyRouteRejectsDuplicateAndOversizedSources(t *testing.T) {
	service := &backupPolicyRouteTestService{}
	server := New(nil, nil, Options{BackupPolicies: service, BackupPolicyMutations: service})
	environmentID := "env_01AAAAAAAAAAAAAAAAAAAAAAAA"
	tests := []struct {
		body   string
		status int
	}{
		{
			body:   `{"enabled":false,"enabled":true,"sources":[]}`,
			status: http.StatusBadRequest,
		},
		{body: oversizedBackupPolicyBody(t), status: http.StatusUnprocessableEntity},
	}
	for _, test := range tests {
		request := httptest.NewRequest(
			http.MethodPut,
			"/api/v1/environments/"+environmentID+"/backup-policy",
			strings.NewReader(test.body),
		)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "0123456789abcdef")
		response := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("status/body = %d, %q", response.Code, response.Body.String())
		}
	}
	if service.setCalls != 0 {
		t.Fatalf("SetBackupPolicy calls = %d", service.setCalls)
	}
}

// Rationale: the typed replacement boundary rejects missing members, nulls,
// unknown fields, invalid configured enums, and non-JSON content types.
func TestBackupPolicyRouteRejectsInvalidTypedReplacement(t *testing.T) {
	service := &backupPolicyRouteTestService{}
	server := New(nil, nil, Options{BackupPolicies: service, BackupPolicyMutations: service})
	path := "/api/v1/environments/env_01AAAAAAAAAAAAAAAAAAAAAAAA/backup-policy"
	tests := []struct {
		body        string
		contentType string
		status      int
	}{
		{body: `{}`, contentType: "application/json", status: http.StatusBadRequest},
		{body: `null`, contentType: "application/json", status: http.StatusBadRequest},
		{body: `{"enabled":false,"sources":null}`, contentType: "application/json", status: http.StatusBadRequest},
		{
			body:        `{"enabled":false,"sources":[],"unknown":true}`,
			contentType: "application/json", status: http.StatusBadRequest,
		},
		{
			body: `{"enabled":false,"sources":[` +
				`{"kind":"config","target_id":"env_01AAAAAAAAAAAAAAAAAAAAAAAA","unknown":true}]}`,
			contentType: "application/json", status: http.StatusBadRequest,
		},
		{
			body:        `{"enabled":false,"sources":[],"encryption":"invalid"}`,
			contentType: "application/json", status: http.StatusUnprocessableEntity,
		},
		{body: `{"enabled":false,"sources":[]}`, contentType: "text/plain", status: http.StatusBadRequest},
	}
	for _, test := range tests {
		request := httptest.NewRequest(http.MethodPut, path, strings.NewReader(test.body))
		request.Header.Set("Content-Type", test.contentType)
		request.Header.Set("Idempotency-Key", "0123456789abcdef")
		response := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf(
				"body %q content type %q: status/body = %d, %q",
				test.body, test.contentType, response.Code, response.Body.String(),
			)
		}
		if test.status == http.StatusBadRequest &&
			!strings.Contains(response.Body.String(), `"code":"validation.failed"`) {
			t.Fatalf("malformed response body = %q", response.Body.String())
		}
	}
	if service.setCalls != 0 {
		t.Fatalf("SetBackupPolicy calls = %d", service.setCalls)
	}
}

// Rationale: semantic idempotency promises the exact committed JSON response,
// including member order and whitespace, rather than an equivalent encoding.
func TestBackupPolicyRoutePreservesProtectedResponseBytes(t *testing.T) {
	representation := []byte(`{ "sources" : [] , "enabled" : false }`)
	service := &backupPolicyRouteTestService{representation: representation}
	server := New(nil, nil, Options{BackupPolicies: service, BackupPolicyMutations: service})
	request := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/environments/env_01AAAAAAAAAAAAAAAAAAAAAAAA/backup-policy",
		strings.NewReader(`{"enabled":false,"sources":[]}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "0123456789abcdef")
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), representation) {
		t.Fatalf("status/body = %d, %q, want %q", response.Code, response.Body.Bytes(), representation)
	}
}

func oversizedBackupPolicyBody(t *testing.T) string {
	t.Helper()
	sources := make([]apiTypes.BackupSourceInput, apiTypes.MaximumBackupPolicySources+1)
	for index := range sources {
		sources[index] = apiTypes.BackupSourceInput{
			Kind: apiTypes.BackupSourceConfig, TargetID: "env_01AAAAAAAAAAAAAAAAAAAAAAAA",
		}
	}
	body, err := json.Marshal(apiTypes.BackupPolicyReplacementRequest{Sources: sources})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

type backupPolicyRouteTestService struct {
	policy         apiTypes.BackupPolicy
	input          apiTypes.BackupPolicyReplacementRequest
	representation []byte
	setCalls       int
}

func (service *backupPolicyRouteTestService) GetBackupPolicy(context.Context, string) (apiTypes.BackupPolicy, error) {
	return service.policy, nil
}

func (service *backupPolicyRouteTestService) SetBackupPolicy(
	_ context.Context,
	_ string,
	input apiTypes.BackupPolicyReplacementRequest,
	_ string,
) (apiTypes.BackupPolicyMutationResult, error) {
	service.setCalls++
	service.input = input
	policy := apiTypes.BackupPolicy{
		Enabled: true, Frequency: "*-*-* 03:15:00", Keep: 2,
		Encryption:  apiTypes.BackupEncryptionAge,
		ConnectorID: "con_01AAAAAAAAAAAAAAAAAAAAAAAA", Sources: []apiTypes.BackupSource{},
	}
	representation := service.representation
	if representation == nil {
		representation = []byte(
			`{"enabled":true,"frequency":"*-*-* 03:15:00","keep":2,` +
				`"encryption":"age","connector_id":"con_01AAAAAAAAAAAAAAAAAAAAAAAA","sources":[]}`,
		)
	}
	return apiTypes.BackupPolicyMutationResult{
		Policy: policy, Representation: representation,
	}, nil
}
