package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testdnsrecords "github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testplatformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
)

// Rationale: checking DNS references and publishing deletion must share an
// atomic fence; otherwise an accepted DNS edit can point to a removed Service.
func TestDNSReferenceRemovalGuardRejectsConcurrentRecord(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newComponentRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	records, err := testplatformcomponents.DefaultPlatformComponents(false)
	if err != nil {
		t.Fatal(err)
	}
	current, err := repository.CreatePlatformComponent(ctx, records[0])
	if err != nil {
		t.Fatal(err)
	}
	at := serviceRecordTestTime()
	serviceID, zoneID := ids.NewAt(ids.KindService, at, 1), ids.NewAt(ids.KindNetwork, at, 2)
	reader := testdnsrecords.NewReader(store)
	conditions, err := reader.RequireDNSUnreferenced(ctx, serviceID, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	current.Record.Desired.Config.CoreDNS.Records = []core.DNSRecord{
		{Hostname: "app.internal", ServiceID: serviceID, ZoneID: zoneID},
	}
	value, err := testcomponents.EncodeRecord(current.Record)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: testcomponents.RecordKey(current.Record.Desired.ID), Value: value}}); err != nil ||
		!result.Succeeded {
		t.Fatalf("add reference = %#v, %v", result, err)
	}
	if result, err := store.Transact(ctx, conditions, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: "/test/removal", Value: []byte("removed")}}); err != nil ||
		result.Succeeded {
		t.Fatalf("removal crossed DNS edit: %#v, %v", result, err)
	}
	for _, target := range []struct{ service, zone string }{{serviceID, ""}, {"", zoneID}} {
		if _, err := reader.RequireDNSUnreferenced(ctx, target.service, target.zone, 0); err == nil {
			t.Fatal("referenced target was removable")
		}
	}
}
