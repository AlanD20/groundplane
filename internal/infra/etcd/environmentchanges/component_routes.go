package environmentchanges

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ComponentTaskRouteCandidate pins one desired Route generation whose
// observation may change only when the owning Component Task succeeds.
type ComponentTaskRouteCandidate struct {
	Desired           core.Route `json:"desired"`
	DesiredGeneration uint64     `json:"desired_generation"`
}

// ComponentTaskRouteProjection is the generic terminal projection contributed
// by a Component that provides the HTTP router capability. A nil Provider means
// successful removal of that capability projects every retained Route unserved.
type ComponentTaskRouteProjection struct {
	Provider *RouteProviderPin             `json:"provider,omitempty"`
	Routes   []ComponentTaskRouteCandidate `json:"routes"`
}

func validateComponentTaskRouteProjection(intent ComponentTaskIntent) error {
	projection := intent.RouteProjection
	if projection == nil {
		return nil
	}
	if projection.Provider != nil {
		if err := ValidateRouteProviderPin(projection.Provider); err != nil {
			return err
		}
		matched := false
		for _, candidate := range intent.Candidates {
			matched = matched || candidate.Candidate.Desired.ID == projection.Provider.ComponentID &&
				candidate.Candidate.Desired.Enabled
		}
		if !matched {
			return errs.New(errs.KindValidationFailed, "Component Route provider is not a candidate")
		}
	}
	previousID := ""
	for _, route := range projection.Routes {
		if route.Desired.ID <= previousID || route.DesiredGeneration == 0 || route.Desired.Validate() != nil {
			return errs.New(errs.KindValidationFailed, "Component Route projection is invalid")
		}
		previousID = route.Desired.ID
	}
	return nil
}

func CloneComponentTaskRouteProjection(source *ComponentTaskRouteProjection) *ComponentTaskRouteProjection {
	if source == nil {
		return nil
	}
	clone := &ComponentTaskRouteProjection{
		Routes: append([]ComponentTaskRouteCandidate(nil), source.Routes...),
	}
	if source.Provider != nil {
		provider := CloneRouteProviderPin(*source.Provider)
		clone.Provider = &provider
	}
	return clone
}
