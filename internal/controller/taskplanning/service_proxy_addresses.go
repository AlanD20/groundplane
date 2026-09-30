package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func PrepareServiceProxyAddresses(
	ctx context.Context,
	planner *networkreservations.Planner,
	projection environmentprojection.EnvironmentComposeProjection,
	services []core.Service,
) (networkreservations.ProxyAddresses, error) {
	zoneInputs := make([]zones.Record, len(projection.DesiredZones))
	for i, source := range projection.DesiredZones {
		zone, err := zones.NewRecord(projection.EnvironmentID, source.Desired)
		if err != nil {
			return networkreservations.ProxyAddresses{}, err
		}
		zoneInputs[i] = zone
	}
	return planner.PrepareProxyAddresses(ctx, zoneInputs, services)
}
