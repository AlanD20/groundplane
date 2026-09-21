package taskplanning

import (
	"testing"
	"time"

	"github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestProjectZoneProjectionRequiresExplicitCanonicalSubnet(t *testing.T) {
	// Rationale: Zone desired state must contain the operator's reproducible
	// subnet decision and never accept Docker-assigned or ambiguous IPAM.
	t.Parallel()
	at := time.Date(2026, 8, 22, 21, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	networkID := ids.NewAt(ids.KindNetwork, at, 2)
	project := &types.Project{Networks: types.Networks{
		"frontend": {
			Internal: true,
			Ipam:     types.IPAMConfig{Config: []*types.IPAMPool{{Subnet: "10.40.10.0/24"}}},
		},
	}}
	zones, err := ProjectZoneProjection(project, testcomposeidentity.Snapshot{
		Networks: []testcomposeidentity.Resource{{ID: networkID, Name: "frontend"}},
	}, core.ZoneOwnerEnvironment, environmentID)
	if err != nil {
		t.Fatalf("ProjectZoneProjection() error = %v", err)
	}
	if len(zones) != 1 || zones[0].ID != networkID || zones[0].Name != "frontend" ||
		zones[0].Subnet != "10.40.10.0/24" || !zones[0].Internal ||
		zones[0].OwnerKind != core.ZoneOwnerEnvironment || zones[0].OwnerID != environmentID {
		t.Fatalf("zones = %#v", zones)
	}
	project.Networks["frontend"] = types.NetworkConfig{}
	if _, err := ProjectZoneProjection(project, testcomposeidentity.Snapshot{
		Networks: []testcomposeidentity.Resource{{ID: networkID, Name: "frontend"}},
	}, core.ZoneOwnerEnvironment, environmentID); err == nil {
		t.Fatal("ProjectZoneProjection() accepted Docker-derived IPAM")
	}
}
