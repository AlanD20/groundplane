package environmentprojection

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceProxyTrustedCIDRs captures only this Service's own Zones shared with
// its Environment Caddy router. Backing networks and other Environments never
// grant forwarding-header authority. Router Zone changes are part of the
// authored Service fingerprint and replace the affected native proxies.
func ServiceProxyTrustedCIDRs(projection EnvironmentComposeProjection, serviceID string) ([]string, error) {
	var routerZones []string
	for _, component := range projection.Components {
		if component.Desired.Kind == core.ComponentKindIngressCaddy && component.Desired.Enabled {
			router, err := componentrecord.ProjectRecord(component)
			if err != nil {
				return nil, err
			}
			routerZones = router.HTTPRouterZoneIDs()
			break
		}
	}
	if len(routerZones) == 0 {
		return nil, nil
	}
	var zones []string
	found := false
	for _, service := range projection.DesiredServices {
		if service.Desired.ID == serviceID {
			zones, found = service.Desired.Zones, true
			break
		}
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "Service forwarding policy has no captured Service")
	}
	var result []string
	for _, zone := range projection.DesiredZones {
		if slices.Contains(zones, zone.Desired.ID) && slices.Contains(routerZones, zone.Desired.ID) &&
			zone.EnvironmentID == projection.EnvironmentID && zone.Desired.OwnerKind == core.ZoneOwnerEnvironment &&
			zone.Desired.OwnerID == projection.EnvironmentID {
			result = append(result, zone.Desired.Subnet)
		}
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}
