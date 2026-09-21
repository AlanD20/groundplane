package network

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeZoneRemovalImpactZones struct {
	zone testkeyvalue.Versioned[testzones.Record]
}

func (fake fakeZoneRemovalImpactZones) GetZone(
	context.Context, string,

) (testkeyvalue.Versioned[testzones.Record], error) {
	return fake.zone, nil
}

type fakeZoneRemovalImpactAttaches struct {
	items []testkeyvalue.Versioned[testattachments.Record]
}

func (fake fakeZoneRemovalImpactAttaches) ListAttachesByBackingNetworkAtRevision(
	context.Context, string, string,

	int64,
) ([]testkeyvalue.Versioned[testattachments.Record], error) {
	return append([]testkeyvalue.Versioned[testattachments.Record](nil), fake.items...), nil
}

type fakeZoneRemovalImpactServices struct {
	items map[string]testkeyvalue.Versioned[testservices.ServiceRecord]
}

func (fake fakeZoneRemovalImpactServices) GetService(
	_ context.Context,
	id string,
) (testkeyvalue.Versioned[testservices.ServiceRecord], error) {
	return fake.items[id], nil
}

func (fakeZoneRemovalImpactServices) ListServices(
	context.Context, string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testservices.ServiceRecord], error) {
	return testkeyvalue.Page[testservices.ServiceRecord]{}, nil
}

type fakeZoneRemovalImpactFacts struct {
	databases map[string]string
}

func (fake fakeZoneRemovalImpactFacts) ResolveRemovalDatabase(
	_ context.Context,
	current testkeyvalue.Versioned[testattachments.Record],
	consume func(string) error,
) error {
	return consume(fake.databases[current.Record.ID])
}

// Rationale: backing-Zone confirmation and its later precondition must share one deterministic,
// complete projection even when repository enumeration order changes.
func TestBackingZoneRemovalImpactIsCompleteAndDeterministic(t *testing.T) {
	t.Parallel()
	zoneID := "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	projectID := "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	firstAttachID := "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	secondAttachID := "att_01BRZ3NDEKTSV4RRFFQ69G5FAV"
	firstServiceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	secondServiceID := "svc_01BRZ3NDEKTSV4RRFFQ69G5FAV"
	firstEnvironmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	secondEnvironmentID := "env_01BRZ3NDEKTSV4RRFFQ69G5FAV"
	service, err := newZoneRemovalImpactService(
		fakeZoneRemovalImpactZones{zone: testkeyvalue.Versioned[testzones.Record]{
			Record: testzones.Record{EnvironmentID: firstEnvironmentID, Desired: core.Zone{
				ID: zoneID, Name: "backing", Subnet: "10.0.0.0/24",
				OwnerKind: core.ZoneOwnerBackingProject, OwnerID: projectID,
			}},
			Revision: 40, ReadRevision: 50,
		}},
		fakeZoneRemovalImpactAttaches{items: []testkeyvalue.Versioned[testattachments.Record]{
			{Record: testattachments.Record{
				ID: secondAttachID, Name: "worker-db", EnvironmentID: secondEnvironmentID,
				BackingProjectID: projectID, BackingNetworkID: zoneID, ServiceID: secondServiceID,
				CredentialAttachID: secondAttachID,
				Status:             core.AttachReady,
			}, Revision: 12, ReadRevision: 50},
			{Record: testattachments.Record{
				ID: firstAttachID, Name: "api-db", EnvironmentID: firstEnvironmentID,
				BackingProjectID: projectID, BackingNetworkID: zoneID, ServiceID: firstServiceID,
				CredentialAttachID: firstAttachID,
				Status:             core.AttachReady,
			}, Revision: 11, ReadRevision: 50},
		}},
		fakeZoneRemovalImpactServices{items: map[string]testkeyvalue.Versioned[testservices.ServiceRecord]{
			firstServiceID: {Record: testservices.ServiceRecord{
				EnvironmentID: firstEnvironmentID, Desired: core.Service{ID: firstServiceID, Name: "api"},
			}, Revision: 21},
			secondServiceID: {Record: testservices.ServiceRecord{
				EnvironmentID: secondEnvironmentID, Desired: core.Service{ID: secondServiceID, Name: "worker"},
			}, Revision: 22},
		}},
		fakeZoneRemovalImpactFacts{databases: map[string]string{
			firstAttachID: "api_4f19", secondAttachID: "worker_8c2d",
		}},
	)
	if err != nil {
		t.Fatalf("newZoneRemovalImpactService() error = %v", err)
	}
	first, err := service.GetZoneRemovalImpact(context.Background(), zoneID)
	if err != nil {
		t.Fatalf("GetZoneRemovalImpact() error = %v", err)
	}
	second, err := service.GetZoneRemovalImpact(context.Background(), zoneID)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("replayed impacts = %#v / %#v, error = %v", first, second, err)
	}
	if first.Mode != apiTypes.ZoneRemovalImpactCascade || first.ImpactToken == "" ||
		len(first.Attaches) != 2 || first.Attaches[0].ID != firstAttachID ||
		len(first.Services) != 2 || first.Services[0].ID != firstServiceID ||
		len(first.Databases) != 2 || first.Databases[0].AttachID != firstAttachID {
		t.Fatalf("GetZoneRemovalImpact() = %#v", first)
	}
}
