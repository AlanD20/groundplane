package componentaction

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredcatalog "github.com/AlanD20/groundplane-registered-components/catalog"
)

// Catalog resolves sealed actions against the registered Component definitions.
type Catalog interface {
	ResolveManagedConfigActionEnvelope(componentsdk.ActionEnvelope) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.ManagedConfigActionRecipe, error)
	ResolveContainerConfigActionEnvelope(componentsdk.ActionEnvelope) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.ContainerConfigActionRecipe, error)
	ResolveDNSResolverObservationActionEnvelope(componentsdk.ActionEnvelope) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.DNSResolverObservationRecipe, error)
}
