package componentregistration

import (
	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (catalog Catalog) ResolveEdgeTunnelObservation(implementation componentsdk.ImplementationKey,
	action componentsdk.ActionID,
) (registeredcatalog.EdgeTunnelObservationRecipe, bool) {
	return catalog.catalog.ResolveEdgeTunnelObservation(implementation, action)
}

func (catalog Catalog) ResolveEdgeTunnelObservationActionEnvelope(
	envelope componentsdk.ActionEnvelope,
) (componentsdk.Definition, componentsdk.ActionDefinition, registeredcatalog.EdgeTunnelObservationRecipe, error) {
	definition, action, recipe, err := catalog.catalog.ResolveEdgeTunnelObservationActionEnvelope(envelope)
	if err != nil {
		return componentsdk.Definition{}, componentsdk.ActionDefinition{}, registeredcatalog.EdgeTunnelObservationRecipe{}, errs.Wrap(
			errs.KindStateConflict,
			err,
		)
	}
	return definition, action, recipe, nil
}
