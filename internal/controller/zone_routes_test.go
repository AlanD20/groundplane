package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeZoneReader struct {
	zone        etcd.Versioned[etcd.ZoneRecord]
	page        etcd.Page[etcd.ZoneRecord]
	wantRequest etcd.PageRequest
	wantEnv     string
	listed      bool
}

func (fake *fakeZoneReader) GetZone(
	context.Context,
	string,
) (etcd.Versioned[etcd.ZoneRecord], error) {
	return fake.zone, nil
}

func (fake *fakeZoneReader) ListZones(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	fake.listed = environmentID == fake.wantEnv && request == fake.wantRequest
	return fake.page, nil
}

// Rationale: the Zone API must expose stable ownership and Environment
// identity while preserving opaque pagination from the durable repository.
func TestZoneRoutesProjectExactPublicRecord(t *testing.T) {
	t.Parallel()
	record := zoneRouteTestRecord()
	request := etcd.PageRequest{Limit: 3, Cursor: "opaque"}
	reader := &fakeZoneReader{
		zone: etcd.Versioned[etcd.ZoneRecord]{Record: record},
		page: etcd.Page[etcd.ZoneRecord]{
			Items: []etcd.Versioned[etcd.ZoneRecord]{{Record: record}}, NextCursor: "next", Revision: 71,
		},
		wantRequest: request, wantEnv: record.EnvironmentID,
	}
	server := &Server{zones: reader}
	page, err := server.listZones(context.Background(), &zoneListInput{
		Environment: record.EnvironmentID, Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil || !reader.listed || len(page.Body.Items) != 1 {
		t.Fatalf("listZones() = %#v, %v, listed %t", page, err, reader.listed)
	}
	want := zoneResponse(record)
	if !reflect.DeepEqual(page.Body.Items[0], want) || page.Body.NextCursor != "next" {
		t.Fatalf("Zone page = %#v, want %#v", page.Body, want)
	}
	shown, err := server.showZone(context.Background(), &zoneShowInput{ID: record.Desired.ID})
	if err != nil || !reflect.DeepEqual(shown.Body, want) {
		t.Fatalf("showZone() = %#v, %v, want %#v", shown, err, want)
	}
}

// Rationale: both Zone reads are operator capabilities, so the generated
// contract must carry their canonical 1:1 operation identities.
func TestZoneOpenAPIContainsReadOperations(t *testing.T) {
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
	if got := contract.Paths["/zones"]["get"].OperationID; got != "zone.list" {
		t.Fatalf("GET /zones operationId = %q, want zone.list", got)
	}
	if got := contract.Paths["/zones/{id}"]["get"].OperationID; got != "zone.show" {
		t.Fatalf("GET /zones/{id} operationId = %q, want zone.show", got)
	}
}

func zoneRouteTestRecord() etcd.ZoneRecord {
	at := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	return etcd.ZoneRecord{
		EnvironmentID: environmentID,
		Desired: core.Zone{
			ID: ids.NewAt(ids.KindNetwork, at, 2), Name: "frontend", Subnet: "10.40.10.0/24",
			Internal: true, OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		},
	}
}
