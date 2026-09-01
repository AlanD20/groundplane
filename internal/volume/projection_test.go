package volume

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestCloneVolumeMutationProjectionPreservesDesiredTopology(t *testing.T) {
	t.Parallel()
	current := etcd.EnvironmentComposeProjection{
		DesiredZones:    []etcd.EnvironmentZoneProjection{{Desired: core.Zone{ID: "net_desired", Name: "private"}}},
		DesiredServices: []etcd.EnvironmentServiceProjection{{Desired: core.Service{ID: "svc_desired", Name: "api"}}},
		DesiredRoutes:   []etcd.EnvironmentRouteProjection{{Desired: core.Route{ID: "route_desired", Path: "/"}}},
		Volumes:         []etcd.EnvironmentVolumeIdentity{{ID: "vol_desired", Slug: "data", Key: "data"}},
	}
	result := cloneVolumeMutationProjection(current)
	if len(result.DesiredZones) != 1 || len(result.DesiredServices) != 1 || len(result.DesiredRoutes) != 1 ||
		len(result.Volumes) != 1 {
		t.Fatalf("cloned Volume projection = %#v", result)
	}
}
