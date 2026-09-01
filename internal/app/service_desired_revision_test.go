package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestCloneEnvironmentDesiredProjectionPreservesDesiredTopology(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	zoneID := ids.NewAt(ids.KindNetwork, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	generatedServiceID := ids.NewAt(ids.KindService, now, 4)
	component, err := etcd.NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 5), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy,
		GeneratedServices: []string{generatedServiceID},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	current := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID,
		DesiredZones: []etcd.EnvironmentZoneProjection{{
			EnvironmentID: environmentID, Desired: core.Zone{ID: zoneID, Name: "frontend"},
		}},
		DesiredServices: []etcd.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: core.Service{ID: serviceID, Name: "api"},
		}, {
			EnvironmentID: environmentID, Desired: core.Service{ID: generatedServiceID, Name: "caddy"},
		}},
		DesiredRoutes: []etcd.EnvironmentRouteProjection{{
			EnvironmentID: environmentID,
			Desired: core.Route{
				ID: ids.NewAt(ids.KindRoute, now, 3), Host: "api.example.test", Path: "/",
				TargetServiceID: serviceID, TargetPort: 8080, Exposure: "public",
			},
			DesiredGeneration: 1,
		}},
		Components: []etcd.ComponentRecord{component},
	}
	result := cloneEnvironmentDesiredProjection(current)
	if len(result.DesiredZones) != 1 || result.DesiredZones[0].Desired.ID != zoneID ||
		len(result.DesiredServices) != 2 || result.DesiredServices[0].Desired.ID != serviceID ||
		result.DesiredServices[1].Desired.ID != generatedServiceID ||
		len(result.Components) != 1 || result.Components[0].Runtime.GeneratedServices[0] != generatedServiceID ||
		len(result.DesiredRoutes) != 1 || result.DesiredRoutes[0].Desired.ID != ids.NewAt(ids.KindRoute, now, 3) {
		t.Fatalf("cloned desired projection = %#v", result)
	}
}
