package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

// ApplyEnvironmentRoute replaces the lossless desired Route and advances generation.
func ApplyEnvironmentRoute(
	current EnvironmentComposeProjection,
	route routerecord.Record,
) (EnvironmentComposeProjection, error) {
	if err := validateEnvironmentComposeProjection(current); err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := routerecord.ValidateRecord(route); err != nil || route.EnvironmentID != current.EnvironmentID {
		return EnvironmentComposeProjection{}, errs.New(
			errs.KindValidationFailed,
			"applied Environment Route is invalid",
		)
	}
	next := cloneEnvironmentComposeProjection(current)
	desired := EnvironmentRouteProjection{
		EnvironmentID: route.EnvironmentID, Desired: route.Desired,
		DesiredGeneration: route.DesiredGeneration,
	}
	desiredReplaced := false
	for index := range next.DesiredRoutes {
		if next.DesiredRoutes[index].Desired.ID == desired.Desired.ID {
			next.DesiredRoutes[index] = desired
			desiredReplaced = true
			break
		}
	}
	if !desiredReplaced {
		next.DesiredRoutes = append(next.DesiredRoutes, desired)
	}
	sort.Slice(next.DesiredRoutes, func(left, right int) bool {
		return next.DesiredRoutes[left].Desired.Host+"\x00"+next.DesiredRoutes[left].Desired.Path <
			next.DesiredRoutes[right].Desired.Host+"\x00"+next.DesiredRoutes[right].Desired.Path
	})
	next.RenderGeneration++
	if err := validateEnvironmentComposeProjectionAdvance(current, true, next); err != nil {
		return EnvironmentComposeProjection{}, err
	}
	return next, nil
}

// RemoveEnvironmentEntry prepares the next applied projection for Entry removal.
// The removed generation remains until terminal acknowledgement promotes next.
func RemoveEnvironmentEntry(
	current EnvironmentComposeProjection,
	entryID string,
) (EnvironmentComposeProjection, bool, error) {
	if err := validateEnvironmentComposeProjection(current); err != nil {
		return EnvironmentComposeProjection{}, false, err
	}
	if recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil {
		return EnvironmentComposeProjection{}, false, errs.New(
			errs.KindValidationFailed,
			"removed Environment Entry id is invalid",
		)
	}
	index := -1
	for candidate := range current.Entries {
		if current.Entries[candidate].Entry.ID == entryID {
			index = candidate
			break
		}
	}
	if index < 0 {
		return cloneEnvironmentComposeProjection(current), false, nil
	}
	next := cloneEnvironmentComposeProjection(current)
	next.Entries = append(next.Entries[:index], next.Entries[index+1:]...)
	next.RenderGeneration++
	if err := validateEnvironmentComposeProjectionAdvance(current, true, next); err != nil {
		return EnvironmentComposeProjection{}, false, err
	}
	return next, true, nil
}

// preserveEnvironmentNonEntryDesiredResources prevents a publication from
// treating omission as deletion. Service, Zone, and Route removals publish
// their candidate head only from their resource-specific successful terminal
// transaction, so a pending Remove-shaped publication is not an exception.
func preserveEnvironmentNonEntryDesiredResources(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
) error {
	if !hasPrevious {
		return nil
	}
	nextIDs := make(map[string]struct{},
		len(next.DesiredServices)+len(next.DesiredZones)+len(next.DesiredRoutes),
	)
	for _, service := range next.DesiredServices {
		nextIDs[service.Desired.ID] = struct{}{}
	}
	for _, zone := range next.DesiredZones {
		nextIDs[zone.Desired.ID] = struct{}{}
	}
	for _, route := range next.DesiredRoutes {
		nextIDs[route.Desired.ID] = struct{}{}
	}
	for _, service := range previous.DesiredServices {
		if _, retained := nextIDs[service.Desired.ID]; !retained {
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing Service %s; remove it explicitly before apply",
				service.Desired.Name,
			)
		}
	}
	for _, zone := range previous.DesiredZones {
		if _, retained := nextIDs[zone.Desired.ID]; !retained {
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing Zone %s; remove it explicitly before apply",
				zone.Desired.Name,
			)
		}
	}
	for _, route := range previous.DesiredRoutes {
		if _, retained := nextIDs[route.Desired.ID]; !retained {
			return errs.New(
				errs.KindResourceInUse,
				"Blueprint omits an existing Route; remove it explicitly before apply",
			)
		}
	}
	return nil
}
