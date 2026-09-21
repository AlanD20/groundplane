package taskplanning

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: source retention belongs to the actual Zone owner; a consumer's
// attached database Network must never be recorded as consumer-owned.
func TestScriptRunnerNetworksSealActualOwners(t *testing.T) {
	const consumerID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAA"
	const backingID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAB"
	const ownedNetworkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAC"
	const backingNetworkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAD"
	sources := testscriptsourcequeries.ScriptExecutionSources{
		Revision: 20, Environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{Record: testhierarchy.EnvironmentRecord{ID: consumerID}},
		DesiredProjection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
					{EnvironmentID: consumerID, Desired: core.Zone{ID: ownedNetworkID, Name: "app"}},
				},
			},
		},
		Networks: []testkeyvalue.Versioned[testzones.Record]{{
			Record:   testzones.Record{EnvironmentID: consumerID, Desired: core.Zone{ID: ownedNetworkID, Name: "app"}},
			Revision: 10, ReadRevision: 20,
		}},
		AttachSources: testscriptsourcequeries.ScriptAttachSources{
			Networks: []testkeyvalue.Versioned[testzones.Record]{{
				Record: testzones.Record{
					EnvironmentID: backingID,
					Desired:       core.Zone{ID: backingNetworkID, Name: "database"},
				},
				Revision: 11, ReadRevision: 20,
			}},
		},
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
