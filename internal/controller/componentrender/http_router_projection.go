package componentrender

import (
	"net/netip"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProjectHTTPRouterInput is the one producer of the scoped HTTP router
// capability snapshot. A registered provider projects its own typed config;
// ownership, reserved addresses and Route reachability do not vary by provider.
func ProjectHTTPRouterInput(
	environment core.Environment,
	instance core.Component,
	zoneIDs []string,
	origin componentsdk.HTTPRouterOrigin,
) (componentsdk.HTTPRouterInput, error) {
	if environment.ID == "" || instance.ID == "" || instance.Owner != core.ComponentOwnerEnvironment ||
		instance.OwnerID != environment.ID || instance.Validate() != nil {
		return componentsdk.HTTPRouterInput{}, errs.New(
			errs.KindValidationFailed,
			"HTTP router Component ownership is invalid",
		)
	}
	if !instance.Enabled {
		return componentsdk.HTTPRouterInput{ComponentID: instance.ID}, nil
	}
	if len(zoneIDs) == 0 {
		return componentsdk.HTTPRouterInput{}, errs.New(
			errs.KindValidationFailed,
			"HTTP router requires a selected Zone",
		)
	}
	zones, err := projectHTTPRouterZones(environment, zoneIDs)
	if err != nil {
		return componentsdk.HTTPRouterInput{}, err
	}
	prefix, err := ipam.ParseIPv4Prefix(environment.Zones[zones[0].Name].Subnet)
	address, addressErr := netip.ParseAddr(instance.PinnedIPv4)
	if err != nil || prefix.String() != environment.Zones[zones[0].Name].Subnet || addressErr != nil ||
		ipam.ValidateUsableIPv4(prefix, address) != nil {
		return componentsdk.HTTPRouterInput{}, errs.New(
			errs.KindValidationFailed,
			"HTTP router pinned IPv4 is not usable in the selected Zone",
		)
	}
	zones[0].StaticIPv4 = instance.PinnedIPv4
	if len(instance.GeneratedServices) != 1 || instance.GeneratedServices[0] == "" {
		return componentsdk.HTTPRouterInput{}, errs.New(
			errs.KindValidationFailed,
			"HTTP router requires one stable generated Service id",
		)
	}
	services := make(map[string]core.Service, len(environment.Services))
	for _, service := range environment.Services {
		services[service.ID] = service
	}
	selectedZoneNames := make(map[string]struct{}, len(zones))
	for _, zone := range zones {
		selectedZoneNames[zone.Name] = struct{}{}
	}
	routes := make([]componentsdk.HTTPRoute, 0, len(environment.Routes))
	for _, route := range environment.Routes {
		if err := route.Validate(); err != nil {
			return componentsdk.HTTPRouterInput{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		service, found := services[route.TargetServiceID]
		if !found || !sharesSelectedZone(service.Zones, selectedZoneNames) ||
			!core.ServiceExposesTCPPort(service.Expose, route.TargetPort) {
			return componentsdk.HTTPRouterInput{}, errs.New(errs.KindValidationFailed,
				"HTTP Route target Service is not reachable through the selected Zone and TCP port")
		}
		routes = append(routes, componentsdk.HTTPRoute{ID: route.ID, Host: route.Host, Path: route.Path,
			BackendServiceID: service.ID, BackendServiceName: service.Name, TargetPort: route.TargetPort,
			Exposure: componentsdk.HTTPRouteExposure(route.Exposure)})
	}
	input := componentsdk.HTTPRouterInput{ComponentID: instance.ID, Enabled: true,
		GeneratedServiceID: instance.GeneratedServices[0], Zones: zones, Origin: origin, Routes: routes}
	if err := componentsdk.ValidateHTTPRouterInput(input); err != nil {
		return componentsdk.HTTPRouterInput{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	return input, nil
}

func projectHTTPRouterZones(
	environment core.Environment,
	zoneIDs []string,
) ([]componentsdk.HTTPRouterZoneInput, error) {
	zones := make([]componentsdk.HTTPRouterZoneInput, len(zoneIDs))
	for index, zoneID := range zoneIDs {
		found := false
		for _, candidate := range environment.Zones {
			if candidate.ID != zoneID {
				continue
			}
			if candidate.OwnerKind != core.ZoneOwnerEnvironment || candidate.OwnerID != environment.ID {
				return nil, errs.New(
					errs.KindScopeUnauthorized,
					"HTTP router selected Zone is not owned by the Environment",
				)
			}
			zones[index] = componentsdk.HTTPRouterZoneInput{ID: candidate.ID, Name: candidate.Name}
			found = true
			break
		}
		if !found {
			return nil, errs.New(errs.KindValidationFailed, "HTTP router selected Zone is not in the Environment")
		}
	}
	return zones, nil
}

func sharesSelectedZone(values []string, selected map[string]struct{}) bool {
	for _, value := range values {
		if _, present := selected[value]; present {
			return true
		}
	}
	return false
}
