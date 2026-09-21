package networkreservations

import (
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"maps"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ZonePoolRegistry struct {
	Reservations map[string]string `json:"reservations"`
}

func ZonePoolRegistryKey(environmentID string) string {
	return "/v1/indexes/zones/by-subnet/environment/" + environmentID
}

func (registry ZonePoolRegistry) Reserve(
	environment hierarchyrecord.EnvironmentRecord,
	zone zonerecord.Record,
) (ZonePoolRegistry, error) {
	parent, err := ipam.ParseIPv4Prefix(environment.NetworkPool)
	if err != nil || parent.String() != environment.NetworkPool {
		return ZonePoolRegistry{}, errs.New(errs.KindValidationFailed, "Environment network pool is invalid")
	}
	candidate, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || candidate.String() != zone.Desired.Subnet {
		return ZonePoolRegistry{}, errs.New(errs.KindValidationFailed, "Zone subnet must be a canonical IPv4 CIDR")
	}
	if current, exists := registry.Reservations[zone.Desired.ID]; exists {
		if current != candidate.String() {
			return ZonePoolRegistry{}, errs.New(
				errs.KindStateConflict,
				"Zone stable identity already reserves a different subnet",
			)
		}
		next := ZonePoolRegistry{Reservations: maps.Clone(registry.Reservations)}
		return next, nil
	}
	reserved, err := registry.Prefixes()
	if err != nil {
		return ZonePoolRegistry{}, err
	}
	if err := ipam.ValidateChild(parent, candidate, reserved); err != nil {
		return ZonePoolRegistry{}, err
	}
	next := ZonePoolRegistry{Reservations: maps.Clone(registry.Reservations)}
	next.Reservations[zone.Desired.ID] = candidate.String()
	return next, nil
}

func (registry ZonePoolRegistry) Release(zone zonerecord.Record) (ZonePoolRegistry, error) {
	if err := ValidateZonePoolRegistry(registry); err != nil {
		return ZonePoolRegistry{}, err
	}
	if err := zonerecord.ValidateRecord(zone); err != nil {
		return ZonePoolRegistry{}, err
	}
	if registry.Reservations[zone.Desired.ID] != zone.Desired.Subnet {
		return ZonePoolRegistry{}, errs.New(errs.KindStateConflict, "Zone subnet reservation changed")
	}
	next := ZonePoolRegistry{Reservations: maps.Clone(registry.Reservations)}
	delete(next.Reservations, zone.Desired.ID)
	return next, nil
}

func (registry ZonePoolRegistry) Prefixes() ([]netip.Prefix, error) {
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	root := netip.MustParsePrefix("0.0.0.0/0")
	for zoneID, value := range registry.Reservations {
		if err := ids.Validate(ids.KindNetwork, zoneID); err != nil {
			return nil, CorruptZonePoolRegistry()
		}
		subnet, err := ipam.ParseIPv4Prefix(value)
		if err != nil || subnet.String() != value || ipam.ValidateChild(root, subnet, reserved) != nil {
			return nil, CorruptZonePoolRegistry()
		}
		reserved = append(reserved, subnet)
	}
	return reserved, nil
}

func ValidateZonePoolRegistry(registry ZonePoolRegistry) error {
	_, err := registry.Prefixes()
	return err
}

func CorruptZonePoolRegistry() error {
	return errs.New(errs.KindInternal, "Zone pool registry is corrupt")
}
