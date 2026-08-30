package app

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"
	registeredcatalog "github.com/AlanD20/groundplane-registered-components/catalog"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const managedConfigActivateAction = componentsdk.ActionID("activate-config")

type registeredActionCatalog struct {
	catalog  registeredcatalog.Catalog
	planners map[componentsdk.ImplementationKey]registeredComponentPlanner
}

type registeredComponentPlanner func(string, componentdns.RenderInput) (componentsdk.EnvironmentPlan, error)

func newRegisteredActionCatalog() (registeredActionCatalog, error) {
	caddy, err := registeredcaddy.Definition()
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	tunnel, err := registeredtunnel.Definition()
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	coreDNS, err := registeredcoredns.Definition()
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	coreDNSActivate, err := registeredcatalog.NewManagedConfigActionRecipe(
		managedConfigActivateAction,
		registeredcoredns.CorefileSource,
	)
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	caddyActivate, err := registeredcatalog.NewContainerConfigActionRecipe(
		registeredcaddy.ActivateConfigAction,
		registeredcaddy.CaddyfileSource,
		registeredcaddy.CaddyfileContainer,
		registeredcaddy.ValidateConfigCommand(),
		registeredcaddy.ActivateConfigCommand(),
		registeredcaddy.Image,
	)
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	compiled, err := registeredcatalog.NewRegistered(
		registeredcatalog.Registration{
			Definition:             caddy,
			ContainerConfigActions: []registeredcatalog.ContainerConfigActionRecipe{caddyActivate},
		},
		registeredcatalog.Registration{Definition: tunnel},
		registeredcatalog.Registration{
			Definition:           coreDNS,
			ManagedConfigActions: []registeredcatalog.ManagedConfigActionRecipe{coreDNSActivate},
		},
	)
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	return registeredActionCatalog{
		catalog: compiled,
		planners: map[componentsdk.ImplementationKey]registeredComponentPlanner{
			coreDNS.Implementation(): func(serviceID string, input componentdns.RenderInput) (componentsdk.EnvironmentPlan, error) {
				return registeredcoredns.Plan(registeredcoredns.PlanInput{
					GeneratedServiceID: serviceID, Render: input,
				})
			},
		},
	}, nil
}

func (catalog registeredActionCatalog) Digest() [32]byte { return catalog.catalog.Digest() }

func (catalog registeredActionCatalog) FindAction(
	implementation componentsdk.ImplementationKey,
	action componentsdk.ActionID,
) (componentsdk.Definition, componentsdk.ActionDefinition, bool) {
	return catalog.catalog.FindAction(implementation, action)
}

func (catalog registeredActionCatalog) FindActionByCapability(
	capability componentsdk.Capability,
	actionID componentsdk.ActionID,
) (componentsdk.Definition, componentsdk.ActionDefinition, bool) {
	for _, definition := range catalog.catalog.Definitions() {
		provides := false
		for _, provided := range definition.Provides() {
			provides = provides || provided == capability
		}
		if !provides {
			continue
		}
		action, found := definition.FindAction(actionID)
		if found {
			return definition, action, true
		}
	}
	return componentsdk.Definition{}, componentsdk.ActionDefinition{}, false
}

func (catalog registeredActionCatalog) Plan(
	implementation componentsdk.ImplementationKey,
	serviceID string,
	input componentdns.RenderInput,
) (componentsdk.EnvironmentPlan, error) {
	planner := catalog.planners[implementation]
	if planner == nil {
		return componentsdk.EnvironmentPlan{}, errs.New(
			errs.KindStateConflict,
			"registered Component implementation is not compiled into the catalog",
		)
	}
	return planner(serviceID, input)
}

func (catalog registeredActionCatalog) ResolveManagedConfigActionEnvelope(
	envelope componentsdk.ActionEnvelope,
) (
	componentsdk.Definition,
	componentsdk.ActionDefinition,
	registeredcatalog.ManagedConfigActionRecipe,
	error,
) {
	definition, action, recipe, err := catalog.catalog.ResolveManagedConfigActionEnvelope(envelope)
	if err != nil {
		return componentsdk.Definition{}, componentsdk.ActionDefinition{},
			registeredcatalog.ManagedConfigActionRecipe{}, errs.Wrap(errs.KindStateConflict, err)
	}
	return definition, action, recipe, nil
}

func (catalog registeredActionCatalog) ResolveActionEnvelope(
	envelope componentsdk.ActionEnvelope,
) (componentsdk.Definition, componentsdk.ActionDefinition, error) {
	definition, action, err := catalog.catalog.ResolveActionEnvelope(envelope)
	if err != nil {
		return componentsdk.Definition{}, componentsdk.ActionDefinition{}, errs.Wrap(errs.KindStateConflict, err)
	}
	return definition, action, nil
}

func (catalog registeredActionCatalog) ResolveContainerConfigActionEnvelope(
	envelope componentsdk.ActionEnvelope,
) (
	componentsdk.Definition,
	componentsdk.ActionDefinition,
	registeredcatalog.ContainerConfigActionRecipe,
	error,
) {
	definition, action, recipe, err := catalog.catalog.ResolveContainerConfigActionEnvelope(envelope)
	if err != nil {
		return componentsdk.Definition{}, componentsdk.ActionDefinition{},
			registeredcatalog.ContainerConfigActionRecipe{}, errs.Wrap(errs.KindStateConflict, err)
	}
	return definition, action, recipe, nil
}

func registeredEnvironmentComponentCatalog() ([]controller.EnvironmentComponentRegistration, error) {
	actionCatalog, err := newRegisteredActionCatalog()
	if err != nil {
		return nil, err
	}
	caddy, err := registeredCaddyEnvironmentComponent(actionCatalog.Digest())
	if err != nil {
		return nil, err
	}
	cloudflareTunnel, err := registeredCloudflareTunnelEnvironmentComponent(actionCatalog.Digest())
	if err != nil {
		return nil, err
	}
	if err := validateRegisteredCoreDNSComponent(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	catalog := []controller.EnvironmentComponentRegistration{caddy, cloudflareTunnel}
	if err := controller.ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, err
	}
	return controller.CloneEnvironmentComponentCatalog(catalog), nil
}
