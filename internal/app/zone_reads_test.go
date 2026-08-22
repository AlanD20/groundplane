package app

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeZoneReadRepository struct {
	zoneReadRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	zone        etcd.Versioned[etcd.ZoneRecord]
	page        etcd.Page[etcd.ZoneRecord]
	wantRequest etcd.PageRequest
	listed      bool
}

func (fake *fakeZoneReadRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeZoneReadRepository) GetZone(
	context.Context,
	string,
) (etcd.Versioned[etcd.ZoneRecord], error) {
	return fake.zone, nil
}

func (fake *fakeZoneReadRepository) ListZones(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

// Rationale: Zone names and subnet reservations are Environment-scoped, so a
// collection read must verify its owner and preserve the repository cursor.
func TestZoneListVerifiesOwnerAndPreservesPagination(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := etcd.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := etcd.Page[etcd.ZoneRecord]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeZoneReadRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record:   etcd.EnvironmentRecord{ID: environmentID, NetworkPool: "10.40.0.0/16"},
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
	want := etcd.Versioned[etcd.ZoneRecord]{
		Record: etcd.ZoneRecord{
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
