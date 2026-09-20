package zones

import (
	"github.com/AlanD20/groundplane/internal/common/composekey"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Record is the joined public view for one Environment-scoped network
// Zone. It is derived from the selected immutable Environment projection and
// is not a durable desired-state authority.
type Record struct {
	EnvironmentID string    `json:"environment_id"`
	Desired       core.Zone `json:"desired"`
}

func NewRecord(environmentID string, desired core.Zone) (Record, error) {
	record := Record{EnvironmentID: environmentID, Desired: desired}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func ValidateRecord(record Record) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindNetwork, record.Desired.ID); err != nil {
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
		if err := recordcodec.ValidateID(ids.KindProject, record.Desired.OwnerID); err != nil {
			return err
		}
	default:
		return errs.New(errs.KindValidationFailed, "Zone owner kind must be environment or backing_project")
	}
	return nil
}
