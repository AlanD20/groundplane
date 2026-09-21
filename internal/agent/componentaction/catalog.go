package componentaction

import (
	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
)

// Catalog resolves sealed actions against the registered Component definitions.
type Catalog interface {
	ResolveManagedConfigActionEnvelope(
		componentsdk.ActionEnvelope,
	) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.ManagedConfigActionRecipe, error)
	ResolveContainerConfigActionEnvelope(
		componentsdk.ActionEnvelope,
	) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.ContainerConfigActionRecipe, error)
	ResolveDNSResolverObservationActionEnvelope(
		componentsdk.ActionEnvelope,
	) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.DNSResolverObservationRecipe, error)
}
