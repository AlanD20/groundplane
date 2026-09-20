package routes

import (
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const RecordPrefix = "/v1/records/routes/"

// Record is the durable boundary for one Environment-scoped ingress
// Route. Host and path remain distinct desired fields as required by the
// Blueprint contract.
type Record struct {
	EnvironmentID     string      `json:"environment_id"`
	Desired           core.Route  `json:"desired"`
	DesiredGeneration uint64      `json:"desired_generation"`
	Observed          Observation `json:"observed"`
}

type ObservedStatus string

const (
	ObservedUnserved ObservedStatus = "unserved"
	ObservedPending  ObservedStatus = "pending"
	ObservedServed   ObservedStatus = "served"
	ObservedDegraded ObservedStatus = "degraded"
)

type ProviderObservation struct {
	ComponentID      string `json:"component_id"`
	DefinitionDigest string `json:"definition_digest"`
	CatalogDigest    string `json:"catalog_digest"`
	InputRevision    int64  `json:"input_revision"`
	InputGeneration  uint64 `json:"input_generation"`
}

type Observation struct {
	Status            ObservedStatus      `json:"status"`
	DesiredGeneration uint64              `json:"desired_generation"`
	Provider          ProviderObservation `json:"provider"`
}

func NewRecord(environmentID string, desired core.Route) (Record, error) {
	record := Record{
		EnvironmentID: environmentID, Desired: desired, DesiredGeneration: 1,
		Observed: Observation{Status: ObservedUnserved, DesiredGeneration: 1},
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

// ReplaceDesired changes exposure while preserving the stable route
// identity, typed match, and target Service selected at creation.
func ReplaceDesired(record Record, desired core.Route) (Record, error) {
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	if desired.ID != record.Desired.ID || desired.Host != record.Desired.Host ||
		desired.Path != record.Desired.Path || desired.TargetServiceID != record.Desired.TargetServiceID ||
		desired.TargetPort != record.Desired.TargetPort {
		return Record{}, errs.New(
			errs.KindValidationFailed,
			"Route replacement changed immutable identity, match, or target Service",
		)
	}
	replacement := record
	replacement.Desired = desired
	if replacement.DesiredGeneration == math.MaxUint64 {
		return Record{}, errs.New(errs.KindStateConflict, "Route desired generation is exhausted")
	}
	replacement.DesiredGeneration++
	replacement.Observed = Observation{
		Status: ObservedUnserved, DesiredGeneration: replacement.DesiredGeneration,
	}
	if err := ValidateRecord(replacement); err != nil {
		return Record{}, err
	}
	return replacement, nil
}

func SetObservation(record Record, observed Observation) (Record, error) {
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	replacement := record
	replacement.Observed = CloneObservation(observed)
	if err := ValidateRecord(replacement); err != nil {
		return Record{}, err
	}
	return replacement, nil
}

func RecordKey(id string) string { return RecordPrefix + id }

func OwnerPrefix(environmentID string) string {
	return "/v1/indexes/routes/by-owner/environment/" + environmentID + "/"
}

func OwnerKey(environmentID string, routeID string) string {
	return OwnerPrefix(environmentID) + routeID
}

func MatchKey(environmentID string, host string, path string) string {
	return "/v1/indexes/routes/by-match/environment/" + environmentID + "/" +
		recordcodec.EncodeKeySegment(host) + "/" + recordcodec.EncodeKeySegment(path)
}

func ValidateRecord(record Record) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindRoute, record.Desired.ID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindService, record.Desired.TargetServiceID); err != nil {
		return err
	}
	if err := record.Desired.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.DesiredGeneration == 0 || record.Observed.DesiredGeneration != record.DesiredGeneration {
		return errs.New(errs.KindValidationFailed, "Route observed generation is invalid")
	}
	switch record.Observed.Status {
	case ObservedUnserved:
		if record.Observed.Provider != (ProviderObservation{}) {
			return errs.New(errs.KindValidationFailed, "unserved Route has a provider observation")
		}
	case ObservedPending, ObservedServed, ObservedDegraded:
		if err := ValidateProviderObservation(&record.Observed.Provider); err != nil {
			return err
		}
	default:
		return errs.New(errs.KindValidationFailed, "Route observed status is invalid")
	}
	return nil
}

func ValidateProviderObservation(provider *ProviderObservation) error {
	if provider == nil || recordcodec.ValidateID(ids.KindComponent, provider.ComponentID) != nil ||
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

func CloneObservation(source Observation) Observation {
	return source
}

func EncodeRecord(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("route", record)
}

func DecodeRecord(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "route")
	if err != nil {
		return Record{}, err
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func CloneRecord(source Record) Record { return source }
