package componentregistration

import (
	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredenvoy "github.com/AlanD20/groundplane-registered-components/envoy"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registeredEnvoyEnvironmentComponent(
	actionCatalog Catalog,
) (componentrender.EnvironmentComponentRegistration, error) {
	definition, err := registeredenvoy.Definition()
	if err != nil {
		return componentrender.EnvironmentComponentRegistration{}, errs.Wrap(errs.KindInternal, err)
	}
	project := func(environment core.Environment, instance core.Component) (componentsdk.HTTPRouterInput, error) {
		input, _, err := componentrender.ProjectEnvoyInput(environment, instance,
			componentsdk.HTTPRouterOrigin{ServiceName: registeredenvoy.ServiceName, URL: registeredenvoy.OriginURL})
		return input, err
	}
	plan := func(input componentsdk.HTTPRouterInput, instance core.Component) (componentsdk.EnvironmentPlan, error) {
		if instance.Kind != core.ComponentKindIngressEnvoy || instance.Validate() != nil ||
			input.ComponentID != instance.ID || input.Enabled != instance.Enabled {
			return componentsdk.EnvironmentPlan{}, errs.New(
				errs.KindValidationFailed, "envoy: captured Component identity or configuration is invalid",
			)
		}
		config := registeredenvoy.Config{}
		if instance.Config.Envoy != nil {
			config.Alias = instance.Config.Envoy.Alias
		}
		result, err := registeredenvoy.Plan(input, config)
		if err != nil {
			return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		if err := actionCatalog.catalog.ValidateEnvironmentPlanImages(definition.Implementation(), result); err != nil {
			return componentsdk.EnvironmentPlan{}, errs.Wrap(errs.KindInternal, err)
		}
		return result, nil
	}
	return componentrender.EnvironmentComponentRegistration{
		Kind: core.ComponentKindIngressEnvoy, Definition: definition, CatalogDigest: actionCatalog.Digest(),
		ManagedConfiguration: &componentrender.EnvironmentManagedConfigurationRegistration{
			SourcePath: registeredenvoy.ConfigurationSource, ActionID: registeredenvoy.ActivateConfigAction,
			Activation: registeredcatalog.ContainerConfigActivationHTTPDiscovery,
		},
		ProjectHTTPRouter: project,
		PlanHTTPRouter:    plan,
		Plan: func(environment core.Environment, instance core.Component) (componentsdk.EnvironmentPlan, error) {
			input, err := project(environment, instance)
			if err != nil {
				return componentsdk.EnvironmentPlan{}, err
			}
			return plan(input, instance)
		},
	}, nil
}
