package componentregistration

import (
	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registeredCaddyEnvironmentComponent(
	actionCatalog Catalog,
) (componentrender.EnvironmentComponentRegistration, error) {
	definition, err := registeredcaddy.Definition()
	if err != nil {
		return componentrender.EnvironmentComponentRegistration{}, errs.Wrap(errs.KindInternal, err)
	}
	return componentrender.EnvironmentComponentRegistration{
		Kind: core.ComponentKindIngressCaddy, Definition: definition, CatalogDigest: actionCatalog.Digest(),
		ValidateAuthoredRoutes: func(spec core.ComponentSpec, routes []core.RouteSpec) error {
			input := make([]componentsdk.HTTPRoute, len(routes))
			for index, route := range routes {
				input[index] = componentsdk.HTTPRoute{Host: route.Hostname, Path: route.Path,
					BackendServiceName: route.Target, TargetPort: route.TargetPort,
					Exposure: componentsdk.HTTPRouteExposure(route.Exposure)}
			}
			if err := registeredcaddy.ValidateRouteTemplate(spec.ImplementationConfig.CaddyfileTemplate, input); err != nil {
				return errs.Wrap(errs.KindRouterConfigurationInvalid, err)
			}
			return nil
		},
		ManagedConfiguration: &componentrender.EnvironmentManagedConfigurationRegistration{
			SourcePath: registeredcaddy.CaddyfileSource,
			ActionID:   registeredcaddy.ActivateConfigAction,
			Activation: registeredcatalog.ContainerConfigActivationCommand,
		},
		Plan: func(environment core.Environment, instance core.Component) (componentsdk.EnvironmentPlan, error) {
			input, config, err := componentrender.ProjectCaddyInput(
				environment,
				instance,
				componentsdk.HTTPRouterOrigin{ServiceName: registeredcaddy.ServiceName, URL: registeredcaddy.OriginURL},
			)
			if err != nil {
				return componentsdk.EnvironmentPlan{}, err
			}
			plan, err := registeredcaddy.Plan(
				input,
				registeredcaddy.Config{CaddyfileTemplate: config.CaddyfileTemplate, Alias: config.Alias},
			)
			if err != nil {
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindRouterConfigurationInvalid, err)
			}
			if err := actionCatalog.catalog.ValidateEnvironmentPlanImages(definition.Implementation(), plan); err != nil {
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindInternal, err)
			}
			return plan, nil
		},
		ProjectHTTPRouter: func(environment core.Environment, instance core.Component) (componentsdk.HTTPRouterInput, error) {
			input, _, err := componentrender.ProjectCaddyInput(
				environment,
				instance,
				componentsdk.HTTPRouterOrigin{ServiceName: registeredcaddy.ServiceName, URL: registeredcaddy.OriginURL},
			)
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
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindRouterConfigurationInvalid, err)
			}
			if err := actionCatalog.catalog.ValidateEnvironmentPlanImages(definition.Implementation(), plan); err != nil {
				return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindInternal, err)
			}
			return plan, nil
		},
	}, nil
}
