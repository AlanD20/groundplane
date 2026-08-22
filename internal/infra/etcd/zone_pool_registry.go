package etcd

import (
	"context"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type zonePoolRegistry struct {
	Reservations map[string]string `json:"reservations"`
}

func zonePoolRegistryKey(environmentID string) string {
	return "/v1/indexes/zones/by-subnet/environment/" + environmentID
}

func (repository *ZoneRepository) getZonePoolRegistry(
	ctx context.Context,
	environmentID string,
) (Versioned[zonePoolRegistry], error) {
	result, err := repository.store.Get(ctx, zonePoolRegistryKey(environmentID))
	if err != nil {
		return Versioned[zonePoolRegistry]{}, err
	}
	if result.Entry == nil {
		return Versioned[zonePoolRegistry]{
			Record: zonePoolRegistry{Reservations: map[string]string{}}, ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := decodeEnvelope[zonePoolRegistry](result.Entry.Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(registry) != nil {
		return Versioned[zonePoolRegistry]{}, corruptZonePoolRegistry()
	}
	return Versioned[zonePoolRegistry]{
		Record: registry, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (registry zonePoolRegistry) reserve(
	environment EnvironmentRecord,
	zone ZoneRecord,
) (zonePoolRegistry, error) {
	parent, err := ipam.ParseIPv4Prefix(environment.NetworkPool)
	if err != nil || parent.String() != environment.NetworkPool {
		return zonePoolRegistry{}, errs.New(errs.KindValidationFailed, "Environment network pool is invalid")
	}
	candidate, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || candidate.String() != zone.Desired.Subnet {
		return zonePoolRegistry{}, errs.New(errs.KindValidationFailed, "Zone subnet must be a canonical IPv4 CIDR")
	}
	if current, exists := registry.Reservations[zone.Desired.ID]; exists {
		if current != candidate.String() {
			return zonePoolRegistry{}, errs.New(
				errs.KindStateConflict,
				"Zone stable identity already reserves a different subnet",
			)
		}
		next := zonePoolRegistry{Reservations: make(map[string]string, len(registry.Reservations))}
		for zoneID, subnet := range registry.Reservations {
			next.Reservations[zoneID] = subnet
		}
		return next, nil
	}
	reserved, err := registry.prefixes()
	if err != nil {
		return zonePoolRegistry{}, err
	}
	if err := ipam.ValidateChild(parent, candidate, reserved); err != nil {
		return zonePoolRegistry{}, err
	}
	next := zonePoolRegistry{Reservations: make(map[string]string, len(registry.Reservations)+1)}
	for zoneID, subnet := range registry.Reservations {
		next.Reservations[zoneID] = subnet
	}
	next.Reservations[zone.Desired.ID] = candidate.String()
	return next, nil
}

func (registry zonePoolRegistry) release(zone ZoneRecord) (zonePoolRegistry, error) {
	if err := validateZonePoolRegistry(registry); err != nil {
		return zonePoolRegistry{}, err
	}
	if err := validateZoneRecord(zone); err != nil {
		return zonePoolRegistry{}, err
	}
	if registry.Reservations[zone.Desired.ID] != zone.Desired.Subnet {
		return zonePoolRegistry{}, errs.New(errs.KindStateConflict, "Zone subnet reservation changed")
	}
	next := zonePoolRegistry{Reservations: make(map[string]string, len(registry.Reservations)-1)}
	for zoneID, subnet := range registry.Reservations {
		if zoneID != zone.Desired.ID {
			next.Reservations[zoneID] = subnet
		}
	}
	return next, nil
}

func (registry zonePoolRegistry) prefixes() ([]netip.Prefix, error) {
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	for zoneID, value := range registry.Reservations {
		if err := ids.Validate(ids.KindNetwork, zoneID); err != nil {
			return nil, corruptZonePoolRegistry()
		}
		subnet, err := ipam.ParseIPv4Prefix(value)
		if err != nil || subnet.String() != value {
			return nil, corruptZonePoolRegistry()
		}
		reserved = append(reserved, subnet)
	}
	return reserved, nil
}

func validateZonePoolRegistry(registry zonePoolRegistry) error {
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	root := netip.MustParsePrefix("0.0.0.0/0")
	for zoneID, value := range registry.Reservations {
		if err := ids.Validate(ids.KindNetwork, zoneID); err != nil {
			return corruptZonePoolRegistry()
		}
		subnet, err := ipam.ParseIPv4Prefix(value)
		if err != nil || subnet.String() != value || ipam.ValidateChild(root, subnet, reserved) != nil {
			return corruptZonePoolRegistry()
		}
		reserved = append(reserved, subnet)
	}
	return nil
}

func corruptZonePoolRegistry() error {
	return errs.New(errs.KindInternal, "Zone pool registry is corrupt")
}
