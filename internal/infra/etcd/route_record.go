package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routePrefix = "/v1/records/routes/"

// RouteRecord is the durable boundary for one Environment-scoped ingress
// Route. Host and path remain distinct desired fields as required by the
// Blueprint contract.
type RouteRecord struct {
	EnvironmentID string     `json:"environment_id"`
	Desired       core.Route `json:"desired"`
}

func NewRouteRecord(environmentID string, desired core.Route) (RouteRecord, error) {
	record := RouteRecord{EnvironmentID: environmentID, Desired: desired}
	if err := validateRouteRecord(record); err != nil {
		return RouteRecord{}, err
	}
	return record, nil
}

// ReplaceRouteDesired changes exposure while preserving the stable route
// identity, typed match, and target Service selected at creation.
func ReplaceRouteDesired(record RouteRecord, desired core.Route) (RouteRecord, error) {
	if err := validateRouteRecord(record); err != nil {
		return RouteRecord{}, err
	}
	if desired.ID != record.Desired.ID || desired.Host != record.Desired.Host ||
		desired.Path != record.Desired.Path || desired.ServiceName != record.Desired.ServiceName {
		return RouteRecord{}, errs.New(
			errs.KindValidationFailed,
			"Route replacement changed immutable identity, match, or target Service",
		)
	}
	replacement := record
	replacement.Desired = desired
	if err := validateRouteRecord(replacement); err != nil {
		return RouteRecord{}, err
	}
	return replacement, nil
}

func routeKey(id string) string { return routePrefix + id }

func routeOwnerPrefix(environmentID string) string {
	return "/v1/indexes/routes/by-owner/environment/" + environmentID + "/"
}

func routeOwnerKey(environmentID string, routeID string) string {
	return routeOwnerPrefix(environmentID) + routeID
}

func validateRouteRecord(record RouteRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindRoute, record.Desired.ID); err != nil {
		return err
	}
	if err := record.Desired.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func encodeRouteRecord(record RouteRecord) ([]byte, error) {
	if err := validateRouteRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("route", record)
}

func decodeRouteRecord(value []byte) (RouteRecord, error) {
	record, err := decodeEnvelope[RouteRecord](value, "route")
	if err != nil {
		return RouteRecord{}, err
	}
	if err := validateRouteRecord(record); err != nil {
		return RouteRecord{}, corruptRecord()
	}
	return record, nil
}
