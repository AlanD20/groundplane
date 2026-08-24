package controller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeConnectorReader struct {
	record          etcd.Versioned[etcd.ConnectorRecord]
	page            etcd.Page[etcd.ConnectorRecord]
	wantEnvironment string
	wantPage        etcd.PageRequest
	listCalls       int
}

func (fake *fakeConnectorReader) GetConnector(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ConnectorRecord], error) {
	return fake.record, nil
}

func (fake *fakeConnectorReader) ListConnectors(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ConnectorRecord], error) {
	fake.listCalls++
	if environmentID != fake.wantEnvironment || request != fake.wantPage {
		return etcd.Page[etcd.ConnectorRecord]{}, io.ErrUnexpectedEOF
	}
	return fake.page, nil
}

type fakeConnectorMutator struct {
	wantEnvironment string
	calls           int
	input           apiTypes.ConnectorCreateRequest
	key             string
	response        etcd.IdempotencyResponse
}

type fakeConnectorDeleter struct {
	wantID   string
	wantKey  string
	response etcd.IdempotencyResponse
	calls    int
}

func (fake *fakeConnectorDeleter) DeleteConnector(
	_ context.Context,
	id string,
	key string,
) (etcd.IdempotencyResponse, error) {
	fake.calls++
	if id != fake.wantID || key != fake.wantKey {
		return etcd.IdempotencyResponse{}, io.ErrUnexpectedEOF
	}
	return fake.response, nil
}

func (fake *fakeConnectorMutator) CreateConnector(
	_ context.Context,
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
	key string,
) (etcd.IdempotencyResponse, error) {
	fake.calls++
	if environmentID != fake.wantEnvironment {
		return etcd.IdempotencyResponse{}, io.ErrUnexpectedEOF
	}
	fake.input = input
	fake.key = key
	return fake.response, nil
}

func TestConnectorRoutesExposeRedactedListAndDetail(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	connectorID := ids.NewAt(ids.KindConnector, now, 2)
	record := etcd.ConnectorRecord{Connector: core.Connector{
		ID: connectorID, EnvironmentID: environmentID, Name: "primary-backups",
		Kind: core.ConnectorKindS3Compatible, Endpoint: "https://objects.example.test",
		Bucket: "groundplane-backups", Prefix: "production/", Region: "auto", PathStyle: true,
		Credentials: map[string]core.ConnectorCredential{
			core.ConnectorCredentialAccessKey: {
				Kind: core.ConnectorCredentialSecretRef, SecretRef: "S3_ACCESS_KEY",
			},
			core.ConnectorCredentialSecretKey: {Kind: core.ConnectorCredentialDirect},
		},
	}}
	reader := &fakeConnectorReader{
		record: etcd.Versioned[etcd.ConnectorRecord]{Record: record},
		page: etcd.Page[etcd.ConnectorRecord]{
			Items: []etcd.Versioned[etcd.ConnectorRecord]{{Record: record}}, NextCursor: "next",
		},
		wantEnvironment: environmentID,
		wantPage:        etcd.PageRequest{Limit: 3, Cursor: "opaque"},
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Connectors: reader})

	list := httptest.NewRecorder()
	server.Mux.ServeHTTP(list, httptest.NewRequest(
		http.MethodGet,
		"/api/v1/connectors?environment="+environmentID+"&limit=3&cursor=opaque",
		nil,
	))
	var page apiTypes.Page[apiTypes.Connector]
	if list.Code != http.StatusOK || json.Unmarshal(list.Body.Bytes(), &page) != nil ||
		reader.listCalls != 1 || len(page.Items) != 1 || page.NextCursor != "next" ||
		page.Items[0].PathStyle != true ||
		page.Items[0].Credentials[core.ConnectorCredentialSecretKey].Kind != apiTypes.ConnectorCredentialDirect ||
		strings.Contains(list.Body.String(), "direct-secret") {
		t.Fatalf("list response = %d/%s, calls %d, page %#v", list.Code, list.Body.String(), reader.listCalls, page)
	}

	detail := httptest.NewRecorder()
	server.Mux.ServeHTTP(
		detail,
		httptest.NewRequest(http.MethodGet, "/api/v1/connectors/"+connectorID, nil),
	)
	var shown apiTypes.Connector
	if detail.Code != http.StatusOK || json.Unmarshal(detail.Body.Bytes(), &shown) != nil ||
		shown.ID != connectorID || shown.Endpoint != record.Connector.Endpoint ||
		strings.Contains(detail.Body.String(), "direct-secret") {
		t.Fatalf("detail response = %d/%s, shown %#v", detail.Code, detail.Body.String(), shown)
	}
}

func TestConnectorCreateRouteRejectsUnknownAndDuplicateJSONMembers(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 21, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	connectorID := ids.NewAt(ids.KindConnector, now, 2)
	created, err := json.Marshal(apiTypes.Connector{ID: connectorID, EnvironmentID: environmentID})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, body := range []string{
		`{"name":"backups","unknown":true}`,
		`{"name":"backups","name":"other"}`,
		`{"name":"backups","credentials":{"access_key":{"secret_ref":"A","secret_ref":"B"}}}`,
	} {
		mutator := &fakeConnectorMutator{
			wantEnvironment: environmentID,
			response: etcd.IdempotencyResponse{
				Status: http.StatusCreated, ContentKind: "application/json", Body: created,
			},
		}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
			ConnectorMutations: mutator,
		})
		request := httptest.NewRequest(
			http.MethodPost,
			"/api/v1/connectors?environment="+environmentID,
			strings.NewReader(body),
		)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "connector-create-key-0001")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || mutator.calls != 0 {
			t.Fatalf("body %s = %d/%s, calls %d", body, response.Code, response.Body.String(), mutator.calls)
		}
	}
}

func TestConnectorCreateRoutePassesCompleteTypedDecision(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 22, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	connectorID := ids.NewAt(ids.KindConnector, now, 2)
	created, err := json.Marshal(apiTypes.Connector{ID: connectorID, EnvironmentID: environmentID})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	mutator := &fakeConnectorMutator{
		wantEnvironment: environmentID,
		response: etcd.IdempotencyResponse{
			Status: http.StatusCreated, ContentKind: "application/json", Body: created,
		},
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		ConnectorMutations: mutator,
	})
	body := `{"name":"backups","kind":"s3-compatible","endpoint":"https://objects.example.test",` +
		`"bucket":"groundplane-backups","prefix":"production/","region":"auto","path_style":false,` +
		`"credentials":{"access_key":{"secret_ref":"S3_ACCESS_KEY"},` +
		`"secret_key":{"value":"direct-secret"}}}`
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/connectors?environment="+environmentID,
		strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "connector-create-key-0002")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || mutator.calls != 1 ||
		mutator.input.PathStyle == nil || *mutator.input.PathStyle ||
		mutator.input.Credentials[core.ConnectorCredentialSecretKey].Value != "direct-secret" ||
		mutator.key != "connector-create-key-0002" {
		t.Fatalf(
			"create response/input/key = %d/%s/%#v/%q",
			response.Code,
			response.Body.String(),
			mutator.input,
			mutator.key,
		)
	}
}

// Rationale: Connector removal is one bodyless protected action, so the route
// must forward the stable target and idempotency key and return the exact Task.
func TestConnectorRemoveRouteDispatchesProtectedFinalizerTask(t *testing.T) {
	t.Parallel()
	connectorID := ids.NewAt(ids.KindConnector, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), 1)
	accepted := apiTypes.TaskAccepted{TaskID: ids.NewAt(
		ids.KindTask,
		time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
		2,
	)}
	body, err := json.Marshal(accepted)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	deleter := &fakeConnectorDeleter{
		wantID: connectorID, wantKey: "connector-delete-key-0003",
		response: etcd.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
		},
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ConnectorDeletions: deleter})
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/connectors/"+connectorID, nil)
	request.Header.Set("Idempotency-Key", deleter.wantKey)
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	var got apiTypes.TaskAccepted
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &got) != nil ||
		got != accepted || deleter.calls != 1 {
		t.Fatalf(
			"remove response = %d/%s, accepted %#v, calls %d",
			response.Code,
			response.Body.String(),
			got,
			deleter.calls,
		)
	}
}

// Rationale: ignored body or query data would create an ambiguous canonical
// DELETE intent, so both transports must fail before application dispatch.
func TestConnectorRemoveRouteRejectsBodyAndQuery(t *testing.T) {
	t.Parallel()
	connectorID := ids.NewAt(ids.KindConnector, time.Date(2026, 8, 24, 12, 30, 0, 0, time.UTC), 1)
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodDelete, "/api/v1/connectors/"+connectorID, strings.NewReader(`{}`)),
		httptest.NewRequest(http.MethodDelete, "/api/v1/connectors/"+connectorID+"?force=true", nil),
	} {
		deleter := &fakeConnectorDeleter{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ConnectorDeletions: deleter})
		request.Header.Set("Idempotency-Key", "connector-delete-key-0004")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || deleter.calls != 0 {
			t.Fatalf("request %s = %d/%s, calls %d", request.URL, response.Code, response.Body.String(), deleter.calls)
		}
	}
}
