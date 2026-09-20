package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routeObservationPrefix = "/v1/records/route-observations/"

// RouteObservationRecord is the mutable runtime sidecar for one desired Route
// generation. It is never part of the immutable Environment head projection.
type RouteObservationRecord struct {
	EnvironmentID     string           `json:"environment_id"`
	RouteID           string           `json:"route_id"`
	DesiredGeneration uint64           `json:"desired_generation"`
	Observation       RouteObservation `json:"observation"`
}

func routeObservationKey(routeID string) string {
	return routeObservationPrefix + routeID
}

func NewRouteObservationRecord(
	environmentID string,
	routeID string,
	desiredGeneration uint64,
	observation RouteObservation,
) (RouteObservationRecord, error) {
	record := RouteObservationRecord{
		EnvironmentID: environmentID, RouteID: routeID,
		DesiredGeneration: desiredGeneration, Observation: cloneRouteObservation(observation),
	}
	if err := validateRouteObservationRecord(record); err != nil {
		return RouteObservationRecord{}, err
	}
	return record, nil
}

func validateRouteObservationRecord(record RouteObservationRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindRoute, record.RouteID); err != nil {
		return err
	}
	if record.DesiredGeneration == 0 || record.Observation.DesiredGeneration != record.DesiredGeneration {
		return errs.New(errs.KindValidationFailed, "Route observation generation is invalid")
	}
	switch record.Observation.Status {
	case RouteObservedUnserved:
		if record.Observation.Provider != (RouteProviderObservation{}) {
			return errs.New(errs.KindValidationFailed, "unserved Route observation has a provider")
		}
	case RouteObservedPending, RouteObservedServed, RouteObservedDegraded:
		if err := validateRouteProviderObservation(&record.Observation.Provider); err != nil {
			return err
		}
	default:
		return errs.New(errs.KindValidationFailed, "Route observation status is invalid")
	}
	return nil
}

func encodeRouteObservation(record RouteObservationRecord) ([]byte, error) {
	if err := validateRouteObservationRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("route_observation", record)
}

func decodeRouteObservation(value []byte) (RouteObservationRecord, error) {
	record, err := recordcodec.Decode[RouteObservationRecord](value, "route_observation")
	if err != nil {
		return RouteObservationRecord{}, err
	}
	if err := validateRouteObservationRecord(record); err != nil {
		return RouteObservationRecord{}, corruptRecord()
	}
	return record, nil
}

func routeObservationAtRevision(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	routeID string,
	desiredGeneration uint64,
	revision int64,
) (RouteObservation, error) {
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeObservationKey(routeID)}, Revision: revision,
	})
	if err != nil {
		return RouteObservation{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision != revision {
		return RouteObservation{}, errs.New(errs.KindInternal, "Route observation read is incomplete")
	}
	if read.Values[0] == nil {
		return RouteObservation{Status: RouteObservedUnserved, DesiredGeneration: desiredGeneration}, nil
	}
	observation, err := decodeRouteObservation(read.Values[0].Value)
	if err != nil {
		return RouteObservation{}, err
	}
	if observation.EnvironmentID != environmentID || observation.RouteID != routeID {
		return RouteObservation{}, corruptRecord()
	}
	if observation.DesiredGeneration != desiredGeneration {
		return RouteObservation{Status: RouteObservedUnserved, DesiredGeneration: desiredGeneration}, nil
	}
	return cloneRouteObservation(observation.Observation), nil
}

func routeRecordFromDesiredProjection(
	ctx context.Context,
	store hierarchyStore,
	projection Versioned[EnvironmentComposeProjection],
	desired EnvironmentRouteProjection,
) (Versioned[RouteRecord], error) {
	if desired.EnvironmentID != projection.Record.EnvironmentID {
		return Versioned[RouteRecord]{}, corruptRecord()
	}
	observation, err := routeObservationAtRevision(
		ctx, store, desired.EnvironmentID, desired.Desired.ID,
		desired.DesiredGeneration, projection.ReadRevision,
	)
	if err != nil {
		return Versioned[RouteRecord]{}, err
	}
	record := RouteRecord{
		EnvironmentID: desired.EnvironmentID, Desired: desired.Desired,
		DesiredGeneration: desired.DesiredGeneration, Observed: observation,
	}
	if err := validateRouteRecord(record); err != nil {
		return Versioned[RouteRecord]{}, corruptRecord()
	}
	return Versioned[RouteRecord]{
		Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
	}, nil
}
