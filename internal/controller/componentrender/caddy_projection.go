package componentrender

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
)

func ProjectCaddyInput(
	environment core.Environment,
	instance core.Component,
	origin componentsdk.HTTPRouterOrigin,
) (componentsdk.HTTPRouterInput, core.CaddyComponentConfig, error) {
	if environment.ID == "" || instance.ID == "" || instance.Owner != core.ComponentOwnerEnvironment ||
		instance.OwnerID != environment.ID || instance.Kind != core.ComponentKindIngressCaddy || instance.Validate() != nil {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.New(
			errs.KindValidationFailed,
			"caddy: component ownership or kind is invalid",
		)
	}
	if !instance.Enabled {
		return componentsdk.HTTPRouterInput{ComponentID: instance.ID}, core.CaddyComponentConfig{}, nil
	}
	if instance.Config.Caddy == nil {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.New(
			errs.KindValidationFailed,
			"caddy: typed config is required",
		)
	}
	zones, err := projectRegisteredCaddyZones(environment, instance.Config.Caddy.ZoneIDs)
	if err != nil {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, err
	}
	prefix, err := ipam.ParseIPv4Prefix(environment.Zones[zones[0].Name].Subnet)
	address, addressErr := netip.ParseAddr(instance.PinnedIPv4)
	if err != nil || prefix.String() != environment.Zones[zones[0].Name].Subnet || addressErr != nil ||
		ipam.ValidateUsableIPv4(prefix, address) != nil {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.New(
			errs.KindValidationFailed,
			"caddy: pinned IPv4 is not usable in the selected Zone",
		)
	}
	zones[0].StaticIPv4 = instance.PinnedIPv4
	if len(instance.GeneratedServices) != 1 || instance.GeneratedServices[0] == "" {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.New(
			errs.KindValidationFailed,
			"caddy: one stable generated Service id is required",
		)
	}
	template := instance.Config.Caddy.CaddyfileTemplate
	services := make(map[string]core.Service, len(environment.Services))
	for _, service := range environment.Services {
		services[service.ID] = service
	}
	routes := make([]componentsdk.HTTPRoute, 0, len(environment.Routes))
	selectedZoneNames := make(map[string]struct{}, len(zones))
	for _, zone := range zones {
		selectedZoneNames[zone.Name] = struct{}{}
	}
	for _, route := range environment.Routes {
		if err := route.Validate(); err != nil {
			return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.Wrap(
				errs.KindValidationFailed,
				err,
			)
		}
		service, found := services[route.TargetServiceID]
		if !found || !sharesSelectedZone(service.Zones, selectedZoneNames) ||
			!core.ServiceExposesTCPPort(service.Expose, route.TargetPort) {
			return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.New(
				errs.KindValidationFailed,
				"caddy: Route target Service is not reachable through the selected Zone and port",
			)
		}
		routes = append(routes, componentsdk.HTTPRoute{
			ID: route.ID, Host: route.Host, Path: route.Path,
			BackendServiceID: service.ID, BackendServiceName: service.Name,
			TargetPort: route.TargetPort, Exposure: componentsdk.HTTPRouteExposure(route.Exposure),
		})
	}
	return componentsdk.HTTPRouterInput{
		ComponentID: instance.ID, Enabled: true, GeneratedServiceID: instance.GeneratedServices[0],
		Zones: zones, Routes: routes,
		Origin: origin,
	}, core.CaddyComponentConfig{CaddyfileTemplate: template, Alias: instance.Config.Caddy.Alias}, nil
}

func projectRegisteredCaddyZones(
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
				return nil, errs.New(errs.KindScopeUnauthorized, "caddy: selected Zone is not owned by the Environment")
			}
			zones[index] = componentsdk.HTTPRouterZoneInput{ID: candidate.ID, Name: candidate.Name}
			found = true
			break
		}
		if !found {
			return nil, errs.New(errs.KindValidationFailed, "caddy: selected Zone is not in the Environment")
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
