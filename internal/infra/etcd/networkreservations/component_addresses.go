package networkreservations

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

type ComponentAddressRegistry struct {
	Reservations map[string]string `json:"reservations"`
}

func ComponentAddressRegistryKey(zoneID string) string {
	return "/v1/indexes/components/by-address/zone/" + zoneID
}

func GetComponentAddressRegistry(
	ctx context.Context,
	store interface {
		Get(context.Context, string) (*etcdstore.GetResult, error)
	},
	zone zonerecord.Record,
) (etcdstore.Versioned[ComponentAddressRegistry], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[ComponentAddressRegistry]{}, err
	}
	if err := zonerecord.ValidateRecord(zone); err != nil {
		return etcdstore.Versioned[ComponentAddressRegistry]{}, err
	}
	result, err := store.Get(ctx, ComponentAddressRegistryKey(zone.Desired.ID))
	if err != nil {
		return etcdstore.Versioned[ComponentAddressRegistry]{}, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[ComponentAddressRegistry]{
			Record:       ComponentAddressRegistry{Reservations: map[string]string{}},
			ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[ComponentAddressRegistry](result.Entry.Value, "component_address_registry")
	if err != nil || ValidateComponentAddressRegistry(zone, registry) != nil {
		return etcdstore.Versioned[ComponentAddressRegistry]{}, CorruptComponentAddressRegistry()
	}
	return etcdstore.Versioned[ComponentAddressRegistry]{
		Record: registry, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (registry ComponentAddressRegistry) Reserve(
	zone zonerecord.Record,
	componentID string,
) (ComponentAddressRegistry, string, error) {
	if err := ValidateComponentAddressRegistry(zone, registry); err != nil {
		return ComponentAddressRegistry{}, "", err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return ComponentAddressRegistry{}, "", errs.New(errs.KindValidationFailed, "Component id is invalid")
	}
	if existing, found := registry.Reservations[componentID]; found {
		return CloneComponentAddressRegistry(registry), existing, nil
	}
	prefix, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || prefix.String() != zone.Desired.Subnet {
		return ComponentAddressRegistry{}, "", errs.New(errs.KindValidationFailed, "Zone subnet is invalid")
	}
	reserved, err := registry.addresses(prefix)
	if err != nil {
		return ComponentAddressRegistry{}, "", err
	}
	address, err := ipam.LastAvailableUsableIPv4(prefix, reserved)
	if err != nil {
		return ComponentAddressRegistry{}, "", err
	}
	next := CloneComponentAddressRegistry(registry)
	next.Reservations[componentID] = address.String()
	return next, address.String(), nil
}

func (registry ComponentAddressRegistry) ReserveExact(
	zone zonerecord.Record,
	componentID string,
	rawAddress string,
) (ComponentAddressRegistry, error) {
	if err := ValidateComponentAddressRegistry(zone, registry); err != nil {
		return ComponentAddressRegistry{}, err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return ComponentAddressRegistry{}, errs.New(errs.KindValidationFailed, "Component id is invalid")
	}
	prefix, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || prefix.String() != zone.Desired.Subnet {
		return ComponentAddressRegistry{}, errs.New(errs.KindValidationFailed, "Zone subnet is invalid")
	}
	address, err := netip.ParseAddr(rawAddress)
	if err != nil || address.String() != rawAddress || ipam.ValidateUsableIPv4(prefix, address) != nil {
		return ComponentAddressRegistry{}, errs.New(errs.KindValidationFailed, "Component address is invalid")
	}
	if existing, found := registry.Reservations[componentID]; found {
		if existing != rawAddress {
			return ComponentAddressRegistry{}, errs.New(
				errs.KindStateConflict,
				"Component already has another address reservation",
			)
		}
		return CloneComponentAddressRegistry(registry), nil
	}
	for ownerID, reserved := range registry.Reservations {
		if reserved == rawAddress && ownerID != componentID {
			return ComponentAddressRegistry{}, errs.New(
				errs.KindStateConflict,
				"Component address is reserved by another Component",
			)
		}
	}
	next := CloneComponentAddressRegistry(registry)
	next.Reservations[componentID] = rawAddress
	return next, nil
}

func (registry ComponentAddressRegistry) Release(
	zone zonerecord.Record,
	componentID string,
) (ComponentAddressRegistry, string, bool, error) {
	if err := ValidateComponentAddressRegistry(zone, registry); err != nil {
		return ComponentAddressRegistry{}, "", false, err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return ComponentAddressRegistry{}, "", false, errs.New(errs.KindValidationFailed, "Component id is invalid")
	}
	next := CloneComponentAddressRegistry(registry)
	address, found := next.Reservations[componentID]
	if found {
		delete(next.Reservations, componentID)
	}
	return next, address, found, nil
}

func (registry ComponentAddressRegistry) addresses(prefix netip.Prefix) ([]netip.Addr, error) {
	addresses := make([]netip.Addr, 0, len(registry.Reservations))
	for componentID, raw := range registry.Reservations {
		if err := ids.Validate(ids.KindComponent, componentID); err != nil {
			return nil, CorruptComponentAddressRegistry()
		}
		address, err := netip.ParseAddr(raw)
		if err != nil || address.String() != raw || ipam.ValidateUsableIPv4(prefix, address) != nil {
			return nil, CorruptComponentAddressRegistry()
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

func ValidateComponentAddressRegistry(zone zonerecord.Record, registry ComponentAddressRegistry) error {
	if err := zonerecord.ValidateRecord(zone); err != nil {
		return err
	}
	prefix, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || prefix.String() != zone.Desired.Subnet {
		return CorruptComponentAddressRegistry()
	}
	addresses, err := registry.addresses(prefix)
	if err != nil {
		return err
	}
	seen := make(map[netip.Addr]struct{}, len(addresses))
	for _, address := range addresses {
		if _, duplicate := seen[address]; duplicate {
			return CorruptComponentAddressRegistry()
		}
		seen[address] = struct{}{}
	}
	return nil
}

func EncodeComponentAddressRegistry(zone zonerecord.Record, registry ComponentAddressRegistry) ([]byte, error) {
	if err := ValidateComponentAddressRegistry(zone, registry); err != nil {
		return nil, err
	}
	return recordcodec.Encode("component_address_registry", registry)
}

func CloneComponentAddressRegistry(registry ComponentAddressRegistry) ComponentAddressRegistry {
	clone := ComponentAddressRegistry{Reservations: make(map[string]string, len(registry.Reservations))}
	for componentID, address := range registry.Reservations {
		clone.Reservations[componentID] = address
	}
	return clone
}

func CorruptComponentAddressRegistry() error {
	return errs.New(errs.KindInternal, "Component address registry is corrupt")
}
