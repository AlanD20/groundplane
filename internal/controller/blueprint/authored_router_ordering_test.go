package blueprint

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Router, Tunnel and Services replace one file baseline. Actual selection must
// serialize those writers without holding independent Volume work.
func TestAuthoredServicesWaitForRouterAndTunnelFileBaseline(t *testing.T) {
	router := blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: ids.New(ids.KindComponent)}
	tunnel := blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: ids.New(ids.KindComponent)}
	service := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.New(ids.KindService)}
	volume := blueprintunits.ResourceKey{Kind: ids.KindVolume, ID: ids.New(ids.KindVolume)}
	capability := string(core.ComponentCapabilityHTTPRouter)
	tunnelCapability := string(core.ComponentCapabilityEdgeTunnel)
	planner := authoredUnitPlanner{
		input: authoredParentInput{
			desired: environmentprojection.EnvironmentDesiredInput{Input: core.BlueprintDesiredInput{
				Components: map[string]core.ComponentSpec{
					capability:       {Enabled: true},
					tunnelCapability: {Enabled: true},
				},
			}},
		},
		identities: map[ids.Kind]map[string]environmentprojection.OwnedIdentity{
			ids.KindComponent: {
				capability:       {ID: router.ID, Name: capability},
				tunnelCapability: {ID: tunnel.ID, Name: tunnelCapability},
			},
		},
	}
	reads, after, err := planner.serviceDependencies("api", composetypes.ServiceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	tunnelReads, tunnelAfter, complete, err := planner.componentDependencies(
		tunnelCapability,
		core.ComponentSpec{Enabled: true},
	)
	if err != nil || !complete {
		t.Fatalf("Tunnel dependencies: %v, %v", complete, err)
	}
	parent := ids.New(ids.KindTask)
	snapshot := blueprintunits.Snapshot{HeadTaskID: parent, Desired: &keyvalue.Versioned[blueprintunits.DesiredPlan]{
		Record: blueprintunits.DesiredPlan{ParentTaskID: parent, Units: []blueprintunits.Unit{
			{Target: router, Writes: []blueprintunits.ResourceKey{router}, Fingerprint: strings.Repeat("a", 64)},
			{
				Target:      tunnel,
				Reads:       tunnelReads,
				After:       tunnelAfter,
				Writes:      []blueprintunits.ResourceKey{tunnel},
				Fingerprint: strings.Repeat("d", 64),
			},
			{
				Target:      service,
				Reads:       reads,
				After:       after,
				Writes:      []blueprintunits.ResourceKey{service},
				Fingerprint: strings.Repeat("b", 64),
			},
			{Target: volume, Writes: []blueprintunits.ResourceKey{volume}, Fingerprint: strings.Repeat("c", 64)},
		}},
	}}
	for _, target := range []blueprintunits.ResourceKey{router, tunnel, service, volume} {
		snapshot.Applied = append(
			snapshot.Applied,
			keyvalue.Versioned[blueprintunits.AppliedRecord]{Record: blueprintunits.AppliedRecord{
				Target: target, State: blueprintunits.Absent,
			}},
		)
	}
	selection, err := blueprintunits.Select(snapshot)
	if err != nil || !containsAuthoredFixtureTarget(selection.Waiting, service) ||
		!containsAuthoredFixtureTarget(selection.Waiting, tunnel) ||
		!containsAuthoredFixtureTarget(
			selection.Ready,
			router,
		) || !containsAuthoredFixtureTarget(selection.Ready, volume) {
		t.Fatalf("before router acknowledgement: %#v, %v", selection, err)
	}
	snapshot.Applied[0] = keyvalue.Versioned[blueprintunits.AppliedRecord]{
		Revision: 1,
		Record: blueprintunits.AppliedRecord{
			Target: router, State: blueprintunits.Applied, Fingerprint: strings.Repeat("a", 64),
		},
	}
	selection, err = blueprintunits.Select(snapshot)
	if err != nil || !containsAuthoredFixtureTarget(selection.Ready, tunnel) ||
		!containsAuthoredFixtureTarget(selection.Waiting, service) {
		t.Fatalf("router must unblock Tunnel before Service: %#v, %v", selection, err)
	}
	snapshot.Applied[1] = keyvalue.Versioned[blueprintunits.AppliedRecord]{
		Revision: 2,
		Record: blueprintunits.AppliedRecord{
			Target: tunnel, State: blueprintunits.Applied, Fingerprint: strings.Repeat("d", 64),
		},
	}
	selection, err = blueprintunits.Select(snapshot)
	if err != nil || !containsAuthoredFixtureTarget(selection.Ready, service) {
		t.Fatalf("Tunnel acknowledgement did not unblock Service: %#v, %v", selection, err)
	}
}
