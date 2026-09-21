package app

import (
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func desiredTopologyProjectionFixture(t *testing.T) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2), RenderGeneration: 1,
	}
	for index := 0; index < 6; index++ {
		zone := core.Zone{
			ID: ids.NewAt(ids.KindNetwork, now, int64(10+index)), Name: fmt.Sprintf("zone-%02d", index),
			Subnet: fmt.Sprintf("10.200.%d.0/24", index), OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		}
		projection.DesiredZones = append(projection.DesiredZones, testenvironmentprojection.EnvironmentZoneProjection{
			EnvironmentID: environmentID, Desired: zone,
		})
	}
	for index := 0; index < 13; index++ {
		service := core.Service{
			ID: ids.NewAt(ids.KindService, now, int64(30+index)), Name: fmt.Sprintf("service-%02d", index),
			Image: fmt.Sprintf("example/service:%d", index),
		}
		backingNetworkID := ""
		if index == 0 {
			service.Adapter = "postgres:16"
			backingNetworkID = projection.DesiredZones[0].Desired.ID
		}
		projection.DesiredServices = append(projection.DesiredServices, testservices.EnvironmentServiceProjection{
			EnvironmentID: environmentID, BackingNetworkID: backingNetworkID, Desired: service,
		})
	}
	for index := 0; index < 6; index++ {
		route := core.Route{
			ID: ids.NewAt(ids.KindRoute, now, int64(50+index)), Host: fmt.Sprintf("route-%02d.example.test", index),
			Path: "/", TargetServiceID: projection.DesiredServices[index].Desired.ID,
			TargetPort: uint16(8000 + index), Exposure: "public",
		}
		projection.DesiredRoutes = append(
			projection.DesiredRoutes,
			testenvironmentprojection.EnvironmentRouteProjection{
				EnvironmentID: environmentID, Desired: route, DesiredGeneration: uint64(index + 1),
			},
		)
	}
	return withTestEnvironmentComposeArtifact(projection)
}
