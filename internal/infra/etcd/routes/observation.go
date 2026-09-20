package routes

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routeObservationPrefix = "/v1/records/route-observations/"

// ObservationRecord is the mutable runtime sidecar for one desired Route
// generation. It is never part of the immutable Environment head projection.
type ObservationRecord struct {
	EnvironmentID     string      `json:"environment_id"`
	RouteID           string      `json:"route_id"`
	DesiredGeneration uint64      `json:"desired_generation"`
	Observation       Observation `json:"observation"`
}

func ObservationKey(routeID string) string {
	return routeObservationPrefix + routeID
}

func NewObservationRecord(
	environmentID string,
	routeID string,
	desiredGeneration uint64,
	observation Observation,
) (ObservationRecord, error) {
	record := ObservationRecord{
		EnvironmentID: environmentID, RouteID: routeID,
		DesiredGeneration: desiredGeneration, Observation: CloneObservation(observation),
	}
	if err := ValidateObservationRecord(record); err != nil {
		return ObservationRecord{}, err
	}
	return record, nil
}

func ValidateObservationRecord(record ObservationRecord) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindRoute, record.RouteID); err != nil {
		return err
	}
	if record.DesiredGeneration == 0 || record.Observation.DesiredGeneration != record.DesiredGeneration {
		return errs.New(errs.KindValidationFailed, "Route observation generation is invalid")
	}
	switch record.Observation.Status {
	case ObservedUnserved:
		if record.Observation.Provider != (ProviderObservation{}) {
			return errs.New(errs.KindValidationFailed, "unserved Route observation has a provider")
		}
	case ObservedPending, ObservedServed, ObservedDegraded:
		if err := ValidateProviderObservation(&record.Observation.Provider); err != nil {
			return err
		}
	default:
		return errs.New(errs.KindValidationFailed, "Route observation status is invalid")
	}
	return nil
}

func EncodeObservation(record ObservationRecord) ([]byte, error) {
	if err := ValidateObservationRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("route_observation", record)
}

func DecodeObservation(value []byte) (ObservationRecord, error) {
	record, err := recordcodec.Decode[ObservationRecord](value, "route_observation")
	if err != nil {
		return ObservationRecord{}, err
	}
	if err := ValidateObservationRecord(record); err != nil {
		return ObservationRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
