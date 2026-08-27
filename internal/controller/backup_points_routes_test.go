package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const testBackupPointsRouteEnvironmentID = "env_01J00000000000000000000000"

type recoveryPointReaderStub struct {
	cursor string
	page   etcd.Page[etcd.BackupRecoveryPointRecord]
}

func (stub *recoveryPointReaderStub) ListRecoveryPoints(
	_context context.Context,
	_environmentID string,
	cursor string,
) (etcd.Page[etcd.BackupRecoveryPointRecord], error) {
	stub.cursor = cursor
	return stub.page, nil
}

// Rationale: only the exact verified public projection may cross the HTTP
// boundary; storage locators, digests, Connector data, and verification time stay private.
func TestRecoveryPointRouteProjectsVerifiedRedactedItems(t *testing.T) {
	createdAt := time.Date(2026, time.August, 27, 12, 0, 0, 123000000, time.UTC)
	reader := &recoveryPointReaderStub{page: etcd.Page[etcd.BackupRecoveryPointRecord]{
		Items: []etcd.Versioned[etcd.BackupRecoveryPointRecord]{{
			Record: etcd.BackupRecoveryPointRecord{BackupRecoveryPointSnapshot: etcd.BackupRecoveryPointSnapshot{
				ID: "rp_01J00000000000000000000000", EnvironmentID: testBackupPointsRouteEnvironmentID,
				SourceID: "spt_01J00000000000000000000000", SourceKind: etcd.BackupRuntimeSourceVolume,
				TargetID: "vol_01J00000000000000000000000", ConnectorID: "con_01J00000000000000000000000",
				ObjectKey: "private-object-key", SourceFormat: etcd.BackupRuntimeFormatVolume,
				Encryption: etcd.BackupRuntimeEncryptionAge, KeyEra: 3, Recipient: "private-recipient",
				SizeBytes: 123, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				CreatedAt: createdAt,
			}, VerifiedAt: createdAt.Add(time.Second)}, Revision: 9, ReadRevision: 9,
		}},
		NextCursor: "opaque-next",
	}}
	server := New(nil, nil, Options{RecoveryPoints: reader})
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/environments/"+testBackupPointsRouteEnvironmentID+"/recovery-points?cursor=opaque-current",
		nil,
	)
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if reader.cursor != "opaque-current" {
		t.Fatalf("reader cursor = %q", reader.cursor)
	}
	var body apiTypes.Page[apiTypes.RecoveryPoint]
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.NextCursor != "opaque-next" {
		t.Fatalf("page = %#v", body)
	}
	item := body.Items[0]
	if item.ID != "rp_01J00000000000000000000000" ||
		item.SourceID != "spt_01J00000000000000000000000" ||
		item.TargetID != "vol_01J00000000000000000000000" ||
		item.SourceKind != apiTypes.BackupSourceVolume || item.CreatedAt != "2026-08-27T12:00:00Z" ||
		item.SizeBytes != 123 || !item.Encrypted || item.KeyEra != 3 ||
		item.Status != apiTypes.RecoveryPointVerified {
		t.Fatalf("public recovery point = %#v", item)
	}
	var raw struct {
		Items []map[string]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	var pageFields map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &pageFields); err != nil {
		t.Fatal(err)
	}
	if _, present := pageFields["$schema"]; present {
		t.Fatalf("page exposes schema link: %s", response.Body.String())
	}
	if _, present := pageFields["revision"]; present {
		t.Fatalf("page exposes fixed revision: %s", response.Body.String())
	}
	for _, field := range []string{
		"environment_id", "connector_id", "object_key", "source_format", "sha256",
		"recipient", "verified_at", "locator", "size",
	} {
		if _, present := raw.Items[0][field]; present {
			t.Errorf("item exposes private or superseded field %q: %#v", field, raw.Items[0])
		}
	}
}

// Rationale: Recovery Points are a verified-only read collection; OpenAPI must
// expose one generated GET operation and no mutation surface.
func TestRecoveryPointOpenAPIContainsOnlyListSurface(t *testing.T) {
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(document, &value); err != nil {
		t.Fatal(err)
	}
	path := value.Paths["/environments/{id}/recovery-points"]
	if _, ok := path["get"]; !ok {
		t.Fatalf("recovery point GET operation missing: %#v", path)
	}
	if _, ok := path["post"]; ok {
		t.Fatalf("recovery point mutation operation unexpectedly present: %#v", path)
	}
}

// Rationale: fixed-revision storage evidence belongs inside the authenticated
// cursor; the public page exposes exactly items and optional next_cursor.
func TestRecoveryPointOpenAPISchemaHasExactPageFields(t *testing.T) {
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(document, &root); err != nil {
		t.Fatal(err)
	}
	var components map[string]json.RawMessage
	if err := json.Unmarshal(root["components"], &components); err != nil {
		t.Fatal(err)
	}
	var schemas map[string]json.RawMessage
	if err := json.Unmarshal(components["schemas"], &schemas); err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]json.RawMessage
	}
	if err := json.Unmarshal(schemas["RecoveryPointPage"], &schema); err != nil {
		t.Fatal(err)
	}
	properties := schema.Properties
	if len(properties) != 2 {
		t.Fatalf("RecoveryPointPage properties = %#v, want exactly items and next_cursor", properties)
	}
	for _, field := range []string{"items", "next_cursor"} {
		if _, present := properties[field]; !present {
			t.Errorf("RecoveryPointPage is missing %q: %#v", field, properties)
		}
	}
	if _, present := properties["revision"]; present {
		t.Fatalf("RecoveryPointPage exposes fixed revision: %#v", properties)
	}
}
