package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ListZoneSubnetReservationsAtRevision returns validated reservations from an
// exact MVCC view without exposing the registry persistence record.
func (r *ZoneRepository) ListZoneSubnetReservationsAtRevision(
	ctx context.Context, id string, at int64,
) ([]string, error) {
	stored, err := r.getZonePoolRegistryAtRevision(ctx, id, at)
	if err != nil {
		return nil, err
	}
	reservations := make([]string, 0, len(stored.Record.Reservations))
	for _, subnet := range stored.Record.Reservations {
		reservations = append(reservations, subnet)
	}
	return reservations, nil
}

type zonePoolRegistry struct {
	Reservations map[string]string `json:"reservations"`
}

func zonePoolRegistryKey(environmentID string) string {
	return "/v1/indexes/zones/by-subnet/environment/" + environmentID
}

func (r *ZoneRepository) getZonePoolRegistry(ctx context.Context, id string) (Versioned[zonePoolRegistry], error) {
	return r.getZonePoolRegistryAtRevision(ctx, id, 0)
}

func (r *ZoneRepository) getZonePoolRegistryAtRevision(
	ctx context.Context,
	id string, revision int64,
) (Versioned[zonePoolRegistry], error) {
	if r == nil || r.store == nil || ids.Validate(ids.KindEnvironment, id) != nil || revision < 0 {
		return Versioned[zonePoolRegistry]{}, errs.New(errs.KindInternal, "Environment capacity read is invalid")
	}
	result, err := r.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{zonePoolRegistryKey(id)}, Revision: revision})
	if err != nil {
		return Versioned[zonePoolRegistry]{}, err
	}
	if len(result.Values) != 1 || result.Values[0] == nil {
		return Versioned[zonePoolRegistry]{
			Record: zonePoolRegistry{Reservations: map[string]string{}}, ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[zonePoolRegistry](result.Values[0].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(registry) != nil {
		return Versioned[zonePoolRegistry]{}, corruptZonePoolRegistry()
	}
	return Versioned[zonePoolRegistry]{
		Record: registry, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (registry zonePoolRegistry) reserve(environment EnvironmentRecord, zone zonerecord.Record) (zonePoolRegistry, error) {
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
		next := zonePoolRegistry{Reservations: cloneStringMap(registry.Reservations)}
		return next, nil
	}
	reserved, err := registry.prefixes()
	if err != nil {
		return zonePoolRegistry{}, err
	}
	if err := ipam.ValidateChild(parent, candidate, reserved); err != nil {
		return zonePoolRegistry{}, err
	}
	next := zonePoolRegistry{Reservations: cloneStringMap(registry.Reservations)}
	next.Reservations[zone.Desired.ID] = candidate.String()
	return next, nil
}

func (registry zonePoolRegistry) release(zone zonerecord.Record) (zonePoolRegistry, error) {
	if err := validateZonePoolRegistry(registry); err != nil {
		return zonePoolRegistry{}, err
	}
	if err := zonerecord.ValidateRecord(zone); err != nil {
		return zonePoolRegistry{}, err
	}
	if registry.Reservations[zone.Desired.ID] != zone.Desired.Subnet {
		return zonePoolRegistry{}, errs.New(errs.KindStateConflict, "Zone subnet reservation changed")
	}
	next := zonePoolRegistry{Reservations: cloneStringMap(registry.Reservations)}
	delete(next.Reservations, zone.Desired.ID)
	return next, nil
}

func (registry zonePoolRegistry) prefixes() ([]netip.Prefix, error) {
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	root := netip.MustParsePrefix("0.0.0.0/0")
	for zoneID, value := range registry.Reservations {
		if err := ids.Validate(ids.KindNetwork, zoneID); err != nil {
			return nil, corruptZonePoolRegistry()
		}
		subnet, err := ipam.ParseIPv4Prefix(value)
		if err != nil || subnet.String() != value || ipam.ValidateChild(root, subnet, reserved) != nil {
			return nil, corruptZonePoolRegistry()
		}
		reserved = append(reserved, subnet)
	}
	return reserved, nil
}

func validateZonePoolRegistry(registry zonePoolRegistry) error {
	_, err := registry.prefixes()
	return err
}

func corruptZonePoolRegistry() error {
	return errs.New(errs.KindInternal, "Zone pool registry is corrupt")
}
