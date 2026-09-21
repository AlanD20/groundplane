package componentregistration

import (
	"github.com/AlanD20/groundplane-component-sdk/component"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registeredCloudflareTunnelEnvironmentComponent(
	actionCatalog Catalog,
) (componentrender.EnvironmentComponentRegistration, error) {
	definition, err := registeredtunnel.Definition()
	if err != nil {
		return componentrender.EnvironmentComponentRegistration{}, errs.Wrap(errs.KindInternal, err)
	}
	return componentrender.EnvironmentComponentRegistration{
		Kind: core.ComponentKindEdgeCloudflare, Definition: definition, CatalogDigest: actionCatalog.Digest(),
		Plan: func(environment core.Environment, instance core.Component) (component.EnvironmentPlan, error) {
			plan, err := componentrender.PlanCloudflareTunnel(
				environment,
				instance,
				func(serviceID, secretID string, zones []component.NetworkInput) (component.EnvironmentPlan, error) {
					return registeredtunnel.Plan(
						registeredtunnel.Input{GeneratedServiceID: serviceID, SecretID: secretID, Zones: zones},
					)
				},
			)
			if err != nil {
				return component.EnvironmentPlan{}, err
			}
			if err := actionCatalog.catalog.ValidateEnvironmentPlanImages(definition.Implementation(), plan); err != nil {
				return component.EnvironmentPlan{}, errs.Wrap(errs.KindInternal, err)
			}
			return plan, nil
		},
	}, nil
}
