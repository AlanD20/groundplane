package environmentprojection

import (
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateEnvironmentEntryProjection(environmentID string, values []entryrecord.Record) error {
	previousID := ""
	for _, value := range values {
		if value.Entry.ID <= previousID || value.EnvironmentID != environmentID ||
			entryrecord.ValidateRecord(value) != nil {
			return errs.New(errs.KindValidationFailed, "Environment Entry projection is invalid or unsorted")
		}
		previousID = value.Entry.ID
	}
	return nil
}

func validateEnvironmentZoneProjections(
	environmentID string,
	values []EnvironmentZoneProjection,
) error {
	previousName := ""
	seenIDs := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.EnvironmentID != environmentID || value.Desired.Name <= previousName ||
			zonerecord.ValidateRecord(zonerecord.Record(value)) != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Zone projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Zone projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		previousName = value.Desired.Name
	}
	return nil
}

func validateEnvironmentServiceProjections(
	environmentID string,
	values []servicerecord.EnvironmentServiceProjection,
) error {
	previousName := ""
	seenIDs := make(map[string]struct{}, len(values))
	for _, value := range values {
		record := servicerecord.ServiceRecord{
			EnvironmentID: value.EnvironmentID, BackingNetworkID: value.BackingNetworkID, Desired: value.Desired,
			Runtime: core.ServiceRuntime{
				ServiceID: value.Desired.ID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
			},
		}
		if value.EnvironmentID != environmentID || value.Desired.Name <= previousName ||
			servicerecord.ValidateServiceRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Service projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Service projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		previousName = value.Desired.Name
	}
	return nil
}

func validateEnvironmentRouteProjections(
	environmentID string,
	values []EnvironmentRouteProjection,
) error {
	previousMatch := ""
	seenIDs := make(map[string]struct{}, len(values))
	for _, value := range values {
		match := value.Desired.Host + "\x00" + value.Desired.Path
		record := routerecord.Record{
			EnvironmentID: value.EnvironmentID, Desired: value.Desired, DesiredGeneration: value.DesiredGeneration,
			Observed: routerecord.Observation{
				Status:            routerecord.ObservedUnserved,
				DesiredGeneration: value.DesiredGeneration,
			},
		}
		if value.EnvironmentID != environmentID || match <= previousMatch || routerecord.ValidateRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Route projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Route projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		previousMatch = match
	}
	return nil
}

func validateEnvironmentComponentProjection(environmentID string, values []componentrecord.Record) error {
	if len(values) == 0 {
		return nil
	}
	if len(values) != 2 {
		return errs.New(errs.KindValidationFailed, "Environment Component projection must contain both singletons")
	}
	previousKind := core.ComponentKind("")
	seenIDs := make(map[string]struct{}, len(values))
	serviceOwners := make(map[string]string)
	for _, value := range values {
		kind := value.Desired.Kind
		if componentrecord.ValidateRecord(value) != nil || value.Desired.Owner != core.ComponentOwnerEnvironment ||
			value.Desired.OwnerID != environmentID || kind <= previousKind ||
			(kind != core.ComponentKindIngressCaddy && kind != core.ComponentKindEdgeCloudflare) {
			return errs.New(errs.KindValidationFailed, "Environment Component projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Component projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		for _, serviceID := range value.Runtime.GeneratedServices {
			if owner, exists := serviceOwners[serviceID]; exists && owner != value.Desired.ID {
				return errs.New(errs.KindValidationFailed, "Environment generated Service ownership is duplicated")
			}
			serviceOwners[serviceID] = value.Desired.ID
		}
		previousKind = kind
	}
	return nil
}
