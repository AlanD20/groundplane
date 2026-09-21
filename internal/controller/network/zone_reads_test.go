package network

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

type fakeZoneReadRepository struct {
	zoneReadRepository
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	zone        testkeyvalue.Versioned[testzones.Record]
	page        testkeyvalue.Page[testzones.Record]
	wantRequest testkeyvalue.PageRequest
	listed      bool
}

func (fake *fakeZoneReadRepository) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeZoneReadRepository) GetZone(
	context.Context, string,

) (testkeyvalue.Versioned[testzones.Record], error) {
	return fake.zone, nil
}

func (fake *fakeZoneReadRepository) ListZones(
	_ context.Context,
	environmentID string,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testzones.Record], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

// Rationale: Zone names and subnet reservations are Environment-scoped, so a
// collection read must verify its owner and preserve the repository cursor.
func TestZoneListVerifiesOwnerAndPreservesPagination(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := testkeyvalue.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := testkeyvalue.Page[testzones.Record]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeZoneReadRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record:   testhierarchy.EnvironmentRecord{ID: environmentID, NetworkPool: "10.40.0.0/16"},
			Revision: 12, ReadRevision: 12,
		},
		page: want, wantRequest: request,
	}
	service, err := newZoneReadService(repository)
	if err != nil {
		t.Fatalf("newZoneReadService() error = %v", err)
	}
	got, err := service.ListZones(context.Background(), environmentID, request)
	if err != nil || !reflect.DeepEqual(got, want) || !repository.listed {
		t.Fatalf("ListZones() = %#v, %v, listed %t", got, err, repository.listed)
	}
}

// Rationale: direct Zone reads use stable ids and must preserve the complete
// durable ownership record rather than resolving mutable labels.
func TestZoneReadPreservesDurableRecord(t *testing.T) {
	t.Parallel()
	want := testkeyvalue.Versioned[testzones.Record]{
		Record: testzones.Record{
			EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Desired:       zoneReadTestZone(),
		},
		Revision: 17, ReadRevision: 17,
	}
	repository := &fakeZoneReadRepository{zone: want}
	service, err := newZoneReadService(repository)
	if err != nil {
		t.Fatalf("newZoneReadService() error = %v", err)
	}
	got, err := service.GetZone(context.Background(), want.Record.Desired.ID)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("GetZone() = %#v, %v, want %#v", got, err, want)
	}
}

func zoneReadTestZone() core.Zone {
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	return core.Zone{
		ID: "net_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: "frontend", Subnet: "10.40.10.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	}
}
