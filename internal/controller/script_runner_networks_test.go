package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: source retention belongs to the actual Zone owner; a consumer's
// attached database Network must never be recorded as consumer-owned.
func TestScriptRunnerNetworksSealActualOwners(t *testing.T) {
	const consumerID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAA"
	const backingID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAB"
	const ownedNetworkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAC"
	const backingNetworkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAD"
	sources := etcd.ScriptExecutionSources{
		Revision: 20, Environment: etcd.Versioned[etcd.EnvironmentRecord]{Record: etcd.EnvironmentRecord{ID: consumerID}},
		DesiredProjection: etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: etcd.EnvironmentComposeProjection{
			DesiredZones: []etcd.EnvironmentZoneProjection{
				{EnvironmentID: consumerID, Desired: core.Zone{ID: ownedNetworkID, Name: "app"}},
			},
		}},
		Networks: []etcd.Versioned[etcd.ZoneRecord]{{
			Record:   etcd.ZoneRecord{EnvironmentID: consumerID, Desired: core.Zone{ID: ownedNetworkID, Name: "app"}},
			Revision: 10, ReadRevision: 20,
		}},
		AttachSources: etcd.ScriptAttachSources{Networks: []etcd.Versioned[etcd.ZoneRecord]{{
			Record: etcd.ZoneRecord{
				EnvironmentID: backingID,
				Desired:       core.Zone{ID: backingNetworkID, Name: "database"},
			},
			Revision: 11, ReadRevision: 20,
		}}},
	}
	networks, err := projectScriptNetworks(
		composetypes.ServiceConfig{Networks: map[string]*composetypes.ServiceNetworkConfig{
			"app": {},
		}},
		sources,
		nil,
	)
	if err != nil || len(networks) != 2 {
		t.Fatalf("project runner Networks = %d, %v", len(networks), err)
	}
	if networks[0].OwnerEnvironmentId != consumerID || networks[1].OwnerEnvironmentId != backingID {
		t.Fatalf("runner Network owners = %q, %q", networks[0].OwnerEnvironmentId, networks[1].OwnerEnvironmentId)
	}
}
