package etcd

import (
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routePrefix = "/v1/records/routes/"

// RouteRecord is the durable boundary for one Environment-scoped ingress
// Route. Host and path remain distinct desired fields as required by the
// Blueprint contract.
type RouteRecord struct {
	EnvironmentID     string           `json:"environment_id"`
	Desired           core.Route       `json:"desired"`
	DesiredGeneration uint64           `json:"desired_generation"`
	Observed          RouteObservation `json:"observed"`
}

type RouteObservedStatus string

const (
	RouteObservedUnserved RouteObservedStatus = "unserved"
	RouteObservedPending  RouteObservedStatus = "pending"
	RouteObservedServed   RouteObservedStatus = "served"
	RouteObservedDegraded RouteObservedStatus = "degraded"
)

type RouteProviderObservation struct {
	ComponentID      string `json:"component_id"`
	DefinitionDigest string `json:"definition_digest"`
	CatalogDigest    string `json:"catalog_digest"`
	InputRevision    int64  `json:"input_revision"`
	InputGeneration  uint64 `json:"input_generation"`
}

type RouteObservation struct {
	Status            RouteObservedStatus      `json:"status"`
	DesiredGeneration uint64                   `json:"desired_generation"`
	Provider          RouteProviderObservation `json:"provider"`
}

func NewRouteRecord(environmentID string, desired core.Route) (RouteRecord, error) {
	record := RouteRecord{
		EnvironmentID: environmentID, Desired: desired, DesiredGeneration: 1,
		Observed: RouteObservation{Status: RouteObservedUnserved, DesiredGeneration: 1},
	}
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
		desired.Path != record.Desired.Path || desired.TargetServiceID != record.Desired.TargetServiceID ||
		desired.TargetPort != record.Desired.TargetPort {
		return RouteRecord{}, errs.New(
			errs.KindValidationFailed,
			"Route replacement changed immutable identity, match, or target Service",
		)
	}
	replacement := record
	replacement.Desired = desired
	if replacement.DesiredGeneration == math.MaxUint64 {
		return RouteRecord{}, errs.New(errs.KindStateConflict, "Route desired generation is exhausted")
	}
	replacement.DesiredGeneration++
	replacement.Observed = RouteObservation{
		Status: RouteObservedUnserved, DesiredGeneration: replacement.DesiredGeneration,
	}
	if err := validateRouteRecord(replacement); err != nil {
		return RouteRecord{}, err
	}
	return replacement, nil
}

func SetRouteObservation(record RouteRecord, observed RouteObservation) (RouteRecord, error) {
	if err := validateRouteRecord(record); err != nil {
		return RouteRecord{}, err
	}
	replacement := record
	replacement.Observed = cloneRouteObservation(observed)
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

func routeMatchKey(environmentID string, host string, path string) string {
	return "/v1/indexes/routes/by-match/environment/" + environmentID + "/" +
		encodeDynamicSegment(host) + "/" + encodeDynamicSegment(path)
}

func validateRouteRecord(record RouteRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindRoute, record.Desired.ID); err != nil {
		return err
	}
	if err := validateID(ids.KindService, record.Desired.TargetServiceID); err != nil {
		return err
	}
	if err := record.Desired.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.DesiredGeneration == 0 || record.Observed.DesiredGeneration != record.DesiredGeneration {
		return errs.New(errs.KindValidationFailed, "Route observed generation is invalid")
	}
	switch record.Observed.Status {
	case RouteObservedUnserved:
		if record.Observed.Provider != (RouteProviderObservation{}) {
			return errs.New(errs.KindValidationFailed, "unserved Route has a provider observation")
		}
	case RouteObservedPending, RouteObservedServed, RouteObservedDegraded:
		if err := validateRouteProviderObservation(&record.Observed.Provider); err != nil {
			return err
		}
	default:
		return errs.New(errs.KindValidationFailed, "Route observed status is invalid")
	}
	return nil
}

func validateRouteProviderObservation(provider *RouteProviderObservation) error {
	if provider == nil || validateStableID(ids.KindComponent, provider.ComponentID) != nil ||
		provider.InputRevision <= 0 || provider.InputGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Route provider observation is invalid")
	}
	for _, digest := range []string{provider.DefinitionDigest, provider.CatalogDigest} {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != 32 {
			return errs.New(errs.KindValidationFailed, "Route provider observation digest is invalid")
		}
	}
	return nil
}

func cloneRouteObservation(source RouteObservation) RouteObservation {
	return source
}

func encodeRouteRecord(record RouteRecord) ([]byte, error) {
	if err := validateRouteRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("route", record)
}

func decodeRouteRecord(value []byte) (RouteRecord, error) {
	record, err := recordcodec.Decode[RouteRecord](value, "route")
	if err != nil {
		return RouteRecord{}, err
	}
	if err := validateRouteRecord(record); err != nil {
		return RouteRecord{}, corruptRecord()
	}
	return record, nil
}
