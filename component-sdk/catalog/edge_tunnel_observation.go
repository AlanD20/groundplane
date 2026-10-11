package catalog

import (
	"fmt"
	"net/netip"
	"net/url"
	"sort"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// EdgeTunnelObservationRecipe describes a connector's read-only readiness
// endpoint. It cannot declare a command, provider credential or remote API.
type EdgeTunnelObservationRecipe struct {
	actionID    component.ActionID
	serviceName string
	image       component.OCIImage
	readyURL    string
}

type registeredEdgeTunnelObservation struct {
	implementation component.ImplementationKey
	recipe         EdgeTunnelObservationRecipe
}

func NewEdgeTunnelObservationRecipe(
	actionID component.ActionID,
	serviceName string,
	image component.OCIImage,
	readyURL string,
) (EdgeTunnelObservationRecipe, error) {
	recipe := EdgeTunnelObservationRecipe{
		actionID:    actionID,
		serviceName: serviceName,
		image:       cloneOCIImage(image),
		readyURL:    readyURL,
	}
	if !recipe.valid() {
		return EdgeTunnelObservationRecipe{}, fmt.Errorf("component catalog: edge tunnel observation recipe is invalid")
	}
	return recipe, nil
}

func (recipe EdgeTunnelObservationRecipe) ActionID() component.ActionID { return recipe.actionID }
func (recipe EdgeTunnelObservationRecipe) ServiceName() string          { return recipe.serviceName }
func (recipe EdgeTunnelObservationRecipe) Image() component.OCIImage {
	return cloneOCIImage(recipe.image)
}
func (recipe EdgeTunnelObservationRecipe) ReadyURL() string { return recipe.readyURL }

func (recipe EdgeTunnelObservationRecipe) valid() bool {
	endpoint, err := url.Parse(recipe.readyURL)
	if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.Path != "/ready" ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.RawPath != "" || endpoint.Opaque != "" || endpoint.ForceQuery {
		return false
	}
	address, err := netip.ParseAddrPort(endpoint.Host)
	return err == nil && address.Addr().IsLoopback() && address.Port() != 0 &&
		recipe.actionID != "" && safeToken(recipe.serviceName) && recipe.image.Validate() == nil
}

func registerEdgeTunnelObservations(
	registrations []Registration,
	images []registeredImage,
) ([]registeredEdgeTunnelObservation, error) {
	observations := make([]registeredEdgeTunnelObservation, 0)
	for _, registration := range registrations {
		definition := registration.Definition
		for _, recipe := range registration.EdgeTunnelObservations {
			action, found := definition.FindAction(recipe.actionID)
			if !found || action.Capability() != component.CapabilityEdgeTunnel ||
				action.Operation() != component.OperationObserve ||
				!recipe.valid() ||
				!registeredImageMatches(images, definition.Implementation(), recipe.image) {
				return nil, fmt.Errorf(
					"component catalog: invalid edge tunnel observation recipe for %q",
					definition.Implementation(),
				)
			}
			observations = append(
				observations,
				registeredEdgeTunnelObservation{implementation: definition.Implementation(), recipe: recipe},
			)
		}
	}
	sort.Slice(observations, func(left, right int) bool {
		if observations[left].implementation != observations[right].implementation {
			return observations[left].implementation < observations[right].implementation
		}
		return observations[left].recipe.actionID < observations[right].recipe.actionID
	})
	for index := 1; index < len(observations); index++ {
		if observations[index].implementation == observations[index-1].implementation &&
			observations[index].recipe.actionID == observations[index-1].recipe.actionID {
			return nil, fmt.Errorf(
				"component catalog: repeated edge tunnel observation recipe for %q",
				observations[index].implementation,
			)
		}
	}
	return observations, nil
}

func (c Catalog) ResolveEdgeTunnelObservation(
	implementation component.ImplementationKey,
	actionID component.ActionID,
) (EdgeTunnelObservationRecipe, bool) {
	index := sort.Search(len(c.edgeTunnelObservations), func(index int) bool {
		candidate := c.edgeTunnelObservations[index]
		return candidate.implementation > implementation ||
			candidate.implementation == implementation && candidate.recipe.actionID >= actionID
	})
	if index == len(c.edgeTunnelObservations) || c.edgeTunnelObservations[index].implementation != implementation ||
		c.edgeTunnelObservations[index].recipe.actionID != actionID {
		return EdgeTunnelObservationRecipe{}, false
	}
	return c.edgeTunnelObservations[index].recipe, true
}

func (c Catalog) ResolveEdgeTunnelObservationActionEnvelope(
	envelope component.ActionEnvelope,
) (component.Definition, component.ActionDefinition, EdgeTunnelObservationRecipe, error) {
	definition, action, err := c.ResolveActionEnvelope(envelope)
	if err != nil {
		return component.Definition{}, component.ActionDefinition{}, EdgeTunnelObservationRecipe{}, err
	}
	recipe, found := c.ResolveEdgeTunnelObservation(definition.Implementation(), action.ID())
	if !found {
		return component.Definition{}, component.ActionDefinition{}, EdgeTunnelObservationRecipe{}, fmt.Errorf(
			"component catalog: edge tunnel observation recipe is not registered for action %q",
			action.ID(),
		)
	}
	return definition, action, recipe, nil
}
