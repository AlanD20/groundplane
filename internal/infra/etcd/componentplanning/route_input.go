package componentplanning

import (
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// WithComponentTaskRouteProjection attaches immutable Route observation input
// to an otherwise complete Component Task preparation without publishing it.
func WithComponentTaskRouteProjection(
	preparation ComponentTaskPreparation,
	routes []routerecord.Record,
	provider *environmentchanges.RouteProviderPin,
) (ComponentTaskPreparation, error) {
	if ComponentTaskPreparationIsZero(preparation) {
		return ComponentTaskPreparation{}, errs.New(errs.KindInternal, "Component Route projection requires a Task")
	}
	projection := &environmentchanges.ComponentTaskRouteProjection{
		Routes: make([]environmentchanges.ComponentTaskRouteCandidate, len(routes)),
	}
	if provider != nil {
		cloned := environmentchanges.CloneRouteProviderPin(*provider)
		projection.Provider = &cloned
	}
	for index, route := range routes {
		if routerecord.ValidateRecord(route) != nil ||
			route.EnvironmentID != preparation.Intent.EnvironmentID {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindInternal,
				"Component Route projection input is invalid",
			)
		}
		projection.Routes[index] = environmentchanges.ComponentTaskRouteCandidate{
			Desired: route.Desired, DesiredGeneration: route.DesiredGeneration,
		}
	}
	sort.Slice(projection.Routes, func(left, right int) bool {
		return projection.Routes[left].Desired.ID < projection.Routes[right].Desired.ID
	})
	result := cloneComponentTaskPreparation(preparation)
	result.Intent.RouteProjection = projection
	if err := ValidateComponentTaskPreparation(result); err != nil {
		return ComponentTaskPreparation{}, err
	}
	return cloneComponentTaskPreparation(result), nil
}
