package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	corenetwork "github.com/AlanD20/groundplane/internal/core/network"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeZoneMutator struct {
	input          corenetwork.CreateZoneRequest
	idempotencyKey string
	response       corenetwork.MutationResponse
}

func (fake *fakeZoneMutator) RemoveZone(
	_ context.Context,
	id string,
	idempotencyKey string,
) (corenetwork.MutationResponse, error) {
	fake.input.EnvironmentID = id
	fake.idempotencyKey = idempotencyKey
	return fake.response, nil
}

func (fake *fakeZoneMutator) CreateZone(
	_ context.Context,
	input corenetwork.CreateZoneRequest,
	idempotencyKey string,
) (corenetwork.MutationResponse, error) {
	fake.input = input
	fake.idempotencyKey = idempotencyKey
	return fake.response, nil
}

type fakeZoneReader struct {
	zone        corenetwork.Zone
	page        corenetwork.Page[corenetwork.Zone]
	wantRequest corenetwork.PageRequest
	wantEnv     string
	listed      bool
}

func (fake *fakeZoneReader) GetZone(
	context.Context,
	string,
) (corenetwork.Zone, error) {
	return fake.zone, nil
}

func (fake *fakeZoneReader) ListZones(
	_ context.Context,
	environmentID string,
	request corenetwork.PageRequest,
) (corenetwork.Page[corenetwork.Zone], error) {
	fake.listed = environmentID == fake.wantEnv && request == fake.wantRequest
	return fake.page, nil
}

// Rationale: the Zone API must expose stable ownership and Environment
// identity while preserving opaque pagination from the durable repository.
func TestZoneRoutesProjectExactPublicRecord(t *testing.T) {
	t.Parallel()
	record := zoneRouteTestRecord()
	request := corenetwork.PageRequest{Limit: 3, Cursor: "opaque"}
	reader := &fakeZoneReader{
		zone: record,
		page: corenetwork.Page[corenetwork.Zone]{
			Items: []corenetwork.Zone{record}, NextCursor: "next",
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
	shown, err := server.showZone(context.Background(), &zoneShowInput{ID: record.ID})
	if err != nil || !reflect.DeepEqual(shown.Body, want) {
		t.Fatalf("showZone() = %#v, %v, want %#v", shown, err, want)
	}
}

func TestZoneCreateRouteForwardsStrictInputAndExactResponse(t *testing.T) {
	// Rationale: Zone creation is one synchronous operator capability, so the
	// HTTP boundary must preserve its exact body and idempotency identity.
	t.Parallel()
	record := zoneRouteTestRecord()
	want := zoneResponse(record)
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal Zone: %v", err)
	}
	mutator := &fakeZoneMutator{response: corenetwork.MutationResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: body,
	}}
	server := &Server{zoneMutations: mutator}
	input := apiTypes.ZoneCreate{
		EnvironmentID: record.EnvironmentID, Name: record.Name,
		Subnet: record.Subnet, Internal: record.Internal,
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal Zone create: %v", err)
	}
	output, err := server.createZone(context.Background(), &zoneCreateInput{
		IdempotencyKey: "zone-create-key-0001", RawBody: raw,
	})
	if err != nil || output.Status != http.StatusCreated || output.ContentType != "application/json" ||
		!reflect.DeepEqual(mutator.input, corenetwork.CreateZoneRequest{
			EnvironmentID: input.EnvironmentID, Name: input.Name, Subnet: input.Subnet, Internal: input.Internal,
		}) || mutator.idempotencyKey != "zone-create-key-0001" {
		t.Fatalf("createZone() = %#v, %v; forwarded %#v/%q", output, err, mutator.input, mutator.idempotencyKey)
	}
}

// Rationale: ordinary Zone removal is one task-backed operator capability, so
// its stable target, idempotency key, and exact 202 response must cross HTTP unchanged.
func TestZoneRemoveRouteForwardsStableTargetAndExactResponse(t *testing.T) {
	t.Parallel()
	record := zoneRouteTestRecord()
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: ids.New(ids.KindTask)})
	if err != nil {
		t.Fatalf("marshal Task accepted: %v", err)
	}
	mutator := &fakeZoneMutator{response: corenetwork.MutationResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}}
	server := &Server{zoneMutations: mutator}
	output, err := server.removeZone(context.Background(), &zoneRemoveInput{
		ID: record.ID, IdempotencyKey: "zone-remove-key-0001",
	})
	if err != nil || output.Status != http.StatusAccepted || output.ContentType != "application/json" ||
		mutator.input.EnvironmentID != record.ID || mutator.idempotencyKey != "zone-remove-key-0001" {
		t.Fatalf(
			"removeZone() = %#v, %v; forwarded %q/%q",
			output,
			err,
			mutator.input.EnvironmentID,
			mutator.idempotencyKey,
		)
	}
}

func TestDecodeZoneCreateRejectsAmbiguousJSON(t *testing.T) {
	// Rationale: idempotency protects one canonical intent, so duplicate,
	// unknown, missing, and incorrectly typed members must fail before hashing.
	t.Parallel()
	for _, body := range []string{
		`{"environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"a","name":"b","subnet":"10.0.0.0/24","internal":false}`,
		`{"environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"a","subnet":"10.0.0.0/24","internal":false,"extra":1}`,
		`{"environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"a","subnet":"10.0.0.0/24"}`,
		`{"environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"a","subnet":"10.0.0.0/24","internal":"false"}`,
	} {
		if _, err := decodeZoneCreate([]byte(body)); err == nil {
			t.Fatalf("decodeZoneCreate(%s) error = nil", body)
		}
	}
}

func zoneRouteTestRecord() corenetwork.Zone {
	at := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	return corenetwork.Zone{
		ID: ids.NewAt(ids.KindNetwork, at, 2), EnvironmentID: environmentID,
		Name: "frontend", Subnet: "10.40.10.0/24", Internal: true,
		OwnerKind: corenetwork.ZoneOwnerEnvironment, OwnerID: environmentID,
	}
}
