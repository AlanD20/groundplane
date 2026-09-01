package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/composekey"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ZoneRecord is the joined public view for one Environment-scoped network
// Zone. It is derived from the selected immutable Environment projection and
// is not a durable desired-state authority.
type ZoneRecord struct {
	EnvironmentID string    `json:"environment_id"`
	Desired       core.Zone `json:"desired"`
}

func joinEnvironmentZone(
	projection Versioned[EnvironmentComposeProjection],
	desired EnvironmentZoneProjection,
) (Versioned[ZoneRecord], error) {
	if desired.EnvironmentID != projection.Record.EnvironmentID {
		return Versioned[ZoneRecord]{}, corruptEnvironmentComposeProjection()
	}
	record, err := NewZoneRecord(desired.EnvironmentID, desired.Desired)
	if err != nil {
		return Versioned[ZoneRecord]{}, corruptEnvironmentComposeProjection()
	}
	return Versioned[ZoneRecord]{
		Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
	}, nil
}

func NewZoneRecord(environmentID string, desired core.Zone) (ZoneRecord, error) {
	record := ZoneRecord{EnvironmentID: environmentID, Desired: desired}
	if err := validateZoneRecord(record); err != nil {
		return ZoneRecord{}, err
	}
	return record, nil
}

func validateZoneRecord(record ZoneRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindNetwork, record.Desired.ID); err != nil {
		return err
	}
	if err := composekey.Validate(record.Desired.Name); err != nil {
		return err
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
