package etcd

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
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
	subnet, err := ipam.ParseIPv4Prefix(record.Desired.Subnet)
	if err != nil || subnet.String() != record.Desired.Subnet {
		return errs.New(errs.KindValidationFailed, "Zone subnet must be a canonical IPv4 CIDR")
	}
	switch record.Desired.OwnerKind {
	case core.ZoneOwnerEnvironment:
		if record.Desired.OwnerID != record.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "Environment-owned Zone owner id is invalid")
		}
	case core.ZoneOwnerBackingProject:
		if err := validateID(ids.KindProject, record.Desired.OwnerID); err != nil {
			return err
		}
	default:
		return errs.New(errs.KindValidationFailed, "Zone owner kind must be environment or backing_project")
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
