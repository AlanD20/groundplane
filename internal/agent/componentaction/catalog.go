package componentaction

import (
	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
)

// Catalog resolves sealed actions against the registered Component definitions.
type Catalog interface {
	ResolveDNSResolverObservationActionEnvelope(
		componentsdk.ActionEnvelope,
	) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.DNSResolverObservationRecipe, error)
	ResolveEdgeTunnelObservationActionEnvelope(
		componentsdk.ActionEnvelope,
	) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.EdgeTunnelObservationRecipe, error)
}
