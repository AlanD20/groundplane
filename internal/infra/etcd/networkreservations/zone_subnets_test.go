package networkreservations

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

// TASK-106: native Blueprint admission uses this reservation owner. Reapply
// retains the subnet; the same identity cannot silently replace bridge IPAM.
// This is reservation logic, not atomic publication or host bridge execution.
func TestZoneReservationRejectsImmutableSubnetMutation(t *testing.T) {
	at := time.Date(2026, 8, 22, 21, 30, 0, 0, time.UTC)
	environment := hierarchy.EnvironmentRecord{
		ID: ids.NewAt(ids.KindEnvironment, at, 1), NetworkPool: "10.40.0.0/16",
	}
	zone, err := zones.NewRecord(environment.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, at, 2), Name: "frontend", Subnet: "10.40.10.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := ZonePoolRegistry{Reservations: map[string]string{zone.Desired.ID: zone.Desired.Subnet}}
	reapplied, err := registry.Reserve(environment, zone)
	if err != nil || reapplied.Reservations[zone.Desired.ID] != zone.Desired.Subnet {
		t.Fatalf("unchanged reservation = %#v, %v", reapplied, err)
	}
	changed := zone
	changed.Desired.Subnet = "10.40.11.0/24"
	if _, err := registry.Reserve(environment, changed); err == nil {
		t.Fatal("stable Zone identity replaced its reserved subnet")
	}
	if registry.Reservations[zone.Desired.ID] != zone.Desired.Subnet {
		t.Fatal("refused replacement changed the admitted reservation")
	}
}
