package componentplanning

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func projectedComponentCandidateZone(component core.Component) (string, bool, error) {
	if !component.Kind.IsEnvironmentProxy() || !component.Enabled {
		return "", false, nil
	}
	zoneIDs := component.HTTPRouterZoneIDs()
	if len(zoneIDs) == 0 || ids.Validate(ids.KindNetwork, zoneIDs[0]) != nil {
		return "", false, errs.New(
			errs.KindValidationFailed,
			"enabled router Component candidate has an invalid Zone",
		)
	}
	return zoneIDs[0], true, nil
}
