package app

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane-component-sdk/component"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registeredCloudflareTunnelEnvironmentComponent(
	catalogDigest [sha256.Size]byte,
	routerCatalog []controller.EnvironmentComponentRegistration,
) (controller.EnvironmentComponentRegistration, error) {
	definition, err := registeredtunnel.Definition()
	if err != nil {
		return controller.EnvironmentComponentRegistration{}, errs.Wrap(errs.KindInternal, err)
	}
	routers := append([]controller.EnvironmentComponentRegistration(nil), routerCatalog...)
	return controller.EnvironmentComponentRegistration{
		Kind: core.ComponentKindEdgeCloudflare, Definition: definition, CatalogDigest: catalogDigest,
		Plan: func(environment core.Environment, instance core.Component) (component.EnvironmentPlan, error) {
			return planRegisteredCloudflareTunnel(environment, instance, routers)
		},
	}, nil
}

func planRegisteredCloudflareTunnel(
	environment core.Environment,
	instance core.Component,
	routerCatalog []controller.EnvironmentComponentRegistration,
) (component.EnvironmentPlan, error) {
	if environment.ID == "" || instance.ID == "" || instance.Owner != core.ComponentOwnerEnvironment ||
		instance.OwnerID != environment.ID || instance.Kind != core.ComponentKindEdgeCloudflare {
		return component.EnvironmentPlan{}, errs.New(errs.KindValidationFailed, "cloudflare tunnel: component ownership or kind is invalid")
	}
	if !instance.Enabled { return component.EnvironmentPlan{}, nil }
	if instance.Config.CloudflareTunnel == nil ||
		ids.Validate(ids.KindSecret, instance.Config.CloudflareTunnel.SecretID) != nil {
		return component.EnvironmentPlan{}, errs.New(errs.KindValidationFailed, "cloudflare tunnel: secret_id is invalid")
	}
	secretID := instance.Config.CloudflareTunnel.SecretID
	if len(instance.GeneratedServices) != 1 || ids.Validate(ids.KindService, instance.GeneratedServices[0]) != nil {
		return component.EnvironmentPlan{}, errs.New(errs.KindValidationFailed, "cloudflare tunnel: one stable generated Service id is required")
	}
	router, err := projectCloudflareTunnelHTTPRouter(environment, routerCatalog)
	if err != nil { return component.EnvironmentPlan{}, err }
	planned, err := registeredtunnel.Plan(registeredtunnel.Input{
		GeneratedServiceID: instance.GeneratedServices[0], RouterOrigin: router.Origin,
		RouterNetworkName: router.ZoneName, SecretID: secretID,
	})
	if err != nil { return component.EnvironmentPlan{}, errs.Wrap(errs.KindValidationFailed, err) }
	return planned, nil
}

func projectCloudflareTunnelHTTPRouter(
	environment core.Environment,
	routerCatalog []controller.EnvironmentComponentRegistration,
) (component.HTTPRouterInput, error) {
	var selected *component.HTTPRouterInput
	for _, instance := range environment.Components {
		if !instance.Enabled {
			continue
		}
		for _, registration := range routerCatalog {
			if registration.Kind != instance.Kind || registration.ProjectHTTPRouter == nil {
				continue
			}
			if selected != nil {
				return component.HTTPRouterInput{}, errs.New(
					errs.KindValidationFailed,
					"cloudflare tunnel: Environment has multiple enabled HTTP routers",
				)
			}
			input, err := registration.ProjectHTTPRouter(environment, instance)
			if err != nil {
				return component.HTTPRouterInput{}, err
			}
			input = component.CloneHTTPRouterInput(input)
			if !input.Enabled || component.ValidateHTTPRouterInput(input) != nil {
				return component.HTTPRouterInput{}, errs.New(
					errs.KindValidationFailed,
					"cloudflare tunnel: HTTP router projection is invalid",
				)
			}
			selected = &input
		}
	}
	if selected == nil {
		return component.HTTPRouterInput{}, errs.New(
			errs.KindValidationFailed,
			"cloudflare tunnel: enabled HTTP router is required",
		)
	}
	return component.CloneHTTPRouterInput(*selected), nil
}
