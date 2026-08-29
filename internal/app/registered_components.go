package app

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"
	registeredcatalog "github.com/AlanD20/groundplane-registered-components/catalog"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type registeredActionCatalog struct {
	catalog registeredcatalog.Catalog
}

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
		coreDNSActivateConfigAction,
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
	)
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	compiled, err := registeredcatalog.NewRegistered(
		registeredcatalog.Registration{
			Definition: caddy,
			ContainerConfigActions: []registeredcatalog.ContainerConfigActionRecipe{caddyActivate},
		},
		registeredcatalog.Registration{Definition: tunnel},
		registeredcatalog.Registration{
			Definition: coreDNS,
			ManagedConfigActions: []registeredcatalog.ManagedConfigActionRecipe{coreDNSActivate},
		},
	)
	if err != nil {
		return registeredActionCatalog{}, errs.Wrap(errs.KindInternal, err)
	}
	return registeredActionCatalog{catalog: compiled}, nil
}

func (catalog registeredActionCatalog) Digest() [32]byte { return catalog.catalog.Digest() }

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
