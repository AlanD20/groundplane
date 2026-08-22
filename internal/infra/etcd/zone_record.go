package etcd

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const zonePrefix = "/v1/records/zones/"

// ZoneRecord is the durable boundary for one Environment-scoped network
// Zone. Desired retains the existing domain projection without interpreting
// its unresolved public ownership label.
type ZoneRecord struct {
	EnvironmentID string    `json:"environment_id"`
	Desired       core.Zone `json:"desired"`
}

func NewZoneRecord(environmentID string, desired core.Zone) (ZoneRecord, error) {
	record := ZoneRecord{EnvironmentID: environmentID, Desired: desired}
	if err := validateZoneRecord(record); err != nil {
		return ZoneRecord{}, err
	}
	return record, nil
}

// ReplaceZoneDesired changes mutable desired fields while preserving the
// stable identity, scoped name, and explicit owner selected at creation.
func ReplaceZoneDesired(record ZoneRecord, desired core.Zone) (ZoneRecord, error) {
	if err := validateZoneRecord(record); err != nil {
		return ZoneRecord{}, err
	}
	if desired.ID != record.Desired.ID || desired.Name != record.Desired.Name ||
		desired.OwnedBy != record.Desired.OwnedBy {
		return ZoneRecord{}, errs.New(
			errs.KindValidationFailed,
			"Zone replacement changed immutable identity, name, or owner",
		)
	}
	replacement := record
	replacement.Desired = desired
	if err := validateZoneRecord(replacement); err != nil {
		return ZoneRecord{}, err
	}
	return replacement, nil
}

func zoneKey(id string) string { return zonePrefix + id }

func zoneNameKey(environmentID string, name string) string {
	return "/v1/indexes/zones/by-name/environment/" + environmentID + "/" + encodeDynamicSegment(name)
}

func zoneOwnerPrefix(environmentID string) string {
	return "/v1/indexes/zones/by-owner/environment/" + environmentID + "/"
}

func zoneOwnerKey(environmentID string, zoneID string) string {
	return zoneOwnerPrefix(environmentID) + zoneID
}

func validateZoneRecord(record ZoneRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindNetwork, record.Desired.ID); err != nil {
		return err
	}
	if strings.TrimSpace(record.Desired.Name) == "" {
		return errs.New(errs.KindValidationFailed, "Zone name is required")
	}
	return nil
}

func encodeZoneRecord(record ZoneRecord) ([]byte, error) {
	if err := validateZoneRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("zone", record)
}

func decodeZoneRecord(value []byte) (ZoneRecord, error) {
	record, err := decodeEnvelope[ZoneRecord](value, "zone")
	if err != nil {
		return ZoneRecord{}, err
	}
	if err := validateZoneRecord(record); err != nil {
		return ZoneRecord{}, corruptRecord()
	}
	return record, nil
}
