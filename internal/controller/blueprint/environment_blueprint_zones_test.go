package blueprint

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func TestPrepareEnvironmentBlueprintZoneChangesRejectsImmutableMutation(t *testing.T) {
	// Rationale: changing bridge IPAM in place must fail before desired state
	// reaches the atomic transaction; replacement is add, move, then remove.
	t.Parallel()
	at := time.Date(2026, 8, 22, 21, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	zoneID := ids.NewAt(ids.KindNetwork, at, 2)
	record, err := testzones.NewRecord(environmentID, core.Zone{
		ID: zoneID, Name: "frontend", Subnet: "10.40.10.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	current := testkeyvalue.Versioned[testzones.Record]{Record: record, Revision: 7, ReadRevision: 9}
	changes, err := prepareEnvironmentBlueprintZoneChanges(
		environmentID,
		[]core.Zone{record.Desired},
		[]testkeyvalue.Versioned[testzones.Record]{current},
	)
	if err != nil || len(changes) != 1 || changes[0].Current == nil {
		t.Fatalf("prepareEnvironmentBlueprintZoneChanges() = %#v, %v", changes, err)
	}
	changed := record.Desired
	changed.Subnet = "10.40.11.0/24"
	if _, err := prepareEnvironmentBlueprintZoneChanges(
		environmentID,
		[]core.Zone{changed},
		[]testkeyvalue.Versioned[testzones.Record]{current},
	); err == nil {
		t.Fatal("prepareEnvironmentBlueprintZoneChanges() accepted an immutable subnet change")
	}
}
