package dnsrecords

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func sameDNSTargets(previous, candidate environmentprojection.EnvironmentComposeProjection) bool {
	return slices.Equal(previous.DesiredZones, candidate.DesiredZones) &&
		slices.EqualFunc(previous.DesiredServices, candidate.DesiredServices,
			func(left, right services.EnvironmentServiceProjection) bool {
				return left.EnvironmentID == right.EnvironmentID &&
					left.Desired.ID == right.Desired.ID && left.Desired.Adapter == right.Desired.Adapter &&
					slices.Equal(left.Desired.Zones, right.Desired.Zones) &&
					slices.Equal(left.Desired.Expose, right.Desired.Expose)
			})
}
