package app

import (
	"net/netip"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"

	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registeredCaddyEnvironmentComponent(
	actionCatalog registeredActionCatalog,
) (controller.EnvironmentComponentRegistration, error) {
	definition, err := registeredcaddy.Definition()
	if err != nil {
		return controller.EnvironmentComponentRegistration{}, errs.Wrap(errs.KindInternal, err)
	}
	return controller.EnvironmentComponentRegistration{
		Kind: core.ComponentKindIngressCaddy, Definition: definition, CatalogDigest: actionCatalog.Digest(),
		ManagedConfiguration: &controller.EnvironmentManagedConfigurationRegistration{
			SourcePath: registeredcaddy.CaddyfileSource,
			ActionID:   registeredcaddy.ActivateConfigAction,
		},
		Plan: func(environment core.Environment, instance core.Component) (componentsdk.EnvironmentPlan, error) {
			input, config, err := projectRegisteredCaddyInput(environment, instance)
			if err != nil {
				return componentsdk.EnvironmentPlan{}, err
			}
			plan, err := registeredcaddy.Plan(input, config)
			if err != nil {
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindValidationFailed, err)
			}
			if err := actionCatalog.catalog.ValidateEnvironmentPlanImages(definition.Implementation(), plan); err != nil {
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindInternal, err)
			}
			return plan, nil
		},
		ProjectHTTPRouter: func(environment core.Environment, instance core.Component) (componentsdk.HTTPRouterInput, error) {
			input, _, err := projectRegisteredCaddyInput(environment, instance)
			return input, err
		},
		PlanHTTPRouter: func(input componentsdk.HTTPRouterInput, instance core.Component) (componentsdk.EnvironmentPlan, error) {
			if instance.Config.Caddy == nil {
				return componentsdk.EnvironmentPlan{}, errs.New(
					errs.KindValidationFailed,
					"caddy: typed config is required",
				)
			}
			plan, err := registeredcaddy.Plan(input, registeredcaddy.Config{
				CaddyfileTemplate: instance.Config.Caddy.CaddyfileTemplate, Alias: instance.Config.Caddy.Alias,
			})
			if err != nil {
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindValidationFailed, err)
			}
			if err := actionCatalog.catalog.ValidateEnvironmentPlanImages(definition.Implementation(), plan); err != nil {
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindInternal, err)
			}
			return plan, nil
		},
	}, nil
}

func projectRegisteredCaddyInput(
	environment core.Environment,
	instance core.Component,
) (componentsdk.HTTPRouterInput, registeredcaddy.Config, error) {
	if environment.ID == "" || instance.ID == "" || instance.Owner != core.ComponentOwnerEnvironment ||
		instance.OwnerID != environment.ID || instance.Kind != core.ComponentKindIngressCaddy || instance.Validate() != nil {
		return componentsdk.HTTPRouterInput{}, registeredcaddy.Config{}, errs.New(
			errs.KindValidationFailed,
			"caddy: component ownership or kind is invalid",
		)
	}
	if !instance.Enabled {
		return componentsdk.HTTPRouterInput{ComponentID: instance.ID}, registeredcaddy.Config{}, nil
	}
	if instance.Config.Caddy == nil {
		return componentsdk.HTTPRouterInput{}, registeredcaddy.Config{}, errs.New(
			errs.KindValidationFailed,
			"caddy: typed config is required",
		)
	}
	zones, err := projectRegisteredCaddyZones(environment, instance.Config.Caddy.ZoneIDs)
	if err != nil {
		return componentsdk.HTTPRouterInput{}, registeredcaddy.Config{}, err
	}
	prefix, err := ipam.ParseIPv4Prefix(environment.Zones[zones[0].Name].Subnet)
	address, addressErr := netip.ParseAddr(instance.PinnedIPv4)
	if err != nil || prefix.String() != environment.Zones[zones[0].Name].Subnet || addressErr != nil ||
		ipam.ValidateUsableIPv4(prefix, address) != nil {
		return componentsdk.HTTPRouterInput{}, registeredcaddy.Config{}, errs.New(
			errs.KindValidationFailed,
			"caddy: pinned IPv4 is not usable in the selected Zone",
		)
	}
	zones[0].StaticIPv4 = instance.PinnedIPv4
	if len(instance.GeneratedServices) != 1 || instance.GeneratedServices[0] == "" {
		return componentsdk.HTTPRouterInput{}, registeredcaddy.Config{}, errs.New(
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
			return componentsdk.HTTPRouterInput{}, registeredcaddy.Config{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		service, found := services[route.TargetServiceID]
		if !found || !sharesSelectedZone(service.Zones, selectedZoneNames) ||
			!core.ServiceExposesTCPPort(service.Expose, route.TargetPort) {
			return componentsdk.HTTPRouterInput{}, registeredcaddy.Config{}, errs.New(
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
		Origin: componentsdk.HTTPRouterOrigin{
			ServiceName: registeredcaddy.ServiceName,
			URL:         registeredcaddy.OriginURL,
		},
	}, registeredcaddy.Config{CaddyfileTemplate: template, Alias: instance.Config.Caddy.Alias}, nil
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
