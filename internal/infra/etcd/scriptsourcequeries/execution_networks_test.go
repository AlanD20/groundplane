package scriptsourcequeries

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: Script execution captures immutable desired Zones with the
// projection's exact revision rather than rereading mutable Zone state.
func TestResolveScriptExecutionNetworksUsesImmutableDesiredZones(t *testing.T) {
	at := time.Date(2026, 9, 1, 2, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	firstID := ids.NewAt(ids.KindNetwork, at, 2)
	secondID := ids.NewAt(ids.KindNetwork, at, 3)
	projection := keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: projectionrecord.EnvironmentComposeProjection{
			EnvironmentID: environmentID,
			RevisionID:    ids.NewAt(ids.KindTask, at, 4),
			DesiredZones: []projectionrecord.EnvironmentZoneProjection{
				{EnvironmentID: environmentID, Desired: scriptExecutionZone(firstID, "app", environmentID)},
				{EnvironmentID: environmentID, Desired: scriptExecutionZone(secondID, "data", environmentID)},
			},
		},
		Revision: 17, ReadRevision: 23,
	}
	networks, err := resolveScriptExecutionNetworks(projection)
	if err != nil {
		t.Fatal(err)
	}
	if len(networks) != 2 || networks[0].Record.Desired.ID != firstID ||
		networks[1].Record.Desired.ID != secondID || networks[0].Revision != projection.Revision ||
		networks[1].ReadRevision != projection.ReadRevision {
		t.Fatalf("resolved immutable Zone projections = %#v", networks)
	}
}

func scriptExecutionZone(id, name, environmentID string) core.Zone {
	return core.Zone{
		ID: id, Name: name, Subnet: "10.40.0.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	}
}
