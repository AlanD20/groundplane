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

type componentAddressRegistry struct {
	Reservations map[string]string `json:"reservations"`
}

func componentAddressRegistryKey(zoneID string) string {
	return "/v1/indexes/components/by-address/zone/" + zoneID
}

func getComponentAddressRegistry(
	ctx context.Context,
	store hierarchyStore,
	zone zonerecord.Record,
) (etcdstore.Versioned[componentAddressRegistry], error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[componentAddressRegistry]{}, err
	}
	if err := zonerecord.ValidateRecord(zone); err != nil {
		return etcdstore.Versioned[componentAddressRegistry]{}, err
	}
	result, err := store.Get(ctx, componentAddressRegistryKey(zone.Desired.ID))
	if err != nil {
		return etcdstore.Versioned[componentAddressRegistry]{}, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[componentAddressRegistry]{
			Record:       componentAddressRegistry{Reservations: map[string]string{}},
			ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[componentAddressRegistry](result.Entry.Value, "component_address_registry")
	if err != nil || validateComponentAddressRegistry(zone, registry) != nil {
		return etcdstore.Versioned[componentAddressRegistry]{}, corruptComponentAddressRegistry()
	}
	return etcdstore.Versioned[componentAddressRegistry]{
		Record: registry, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (registry componentAddressRegistry) reserve(
	zone zonerecord.Record,
	componentID string,
) (componentAddressRegistry, string, error) {
	if err := validateComponentAddressRegistry(zone, registry); err != nil {
		return componentAddressRegistry{}, "", err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return componentAddressRegistry{}, "", errs.New(errs.KindValidationFailed, "Component id is invalid")
	}
	if existing, found := registry.Reservations[componentID]; found {
		return cloneComponentAddressRegistry(registry), existing, nil
	}
	prefix, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || prefix.String() != zone.Desired.Subnet {
		return componentAddressRegistry{}, "", errs.New(errs.KindValidationFailed, "Zone subnet is invalid")
	}
	reserved, err := registry.addresses(prefix)
	if err != nil {
		return componentAddressRegistry{}, "", err
	}
	address, err := ipam.LastAvailableUsableIPv4(prefix, reserved)
	if err != nil {
		return componentAddressRegistry{}, "", err
	}
	next := cloneComponentAddressRegistry(registry)
	next.Reservations[componentID] = address.String()
	return next, address.String(), nil
}

func (registry componentAddressRegistry) reserveExact(
	zone zonerecord.Record,
	componentID string,
	rawAddress string,
) (componentAddressRegistry, error) {
	if err := validateComponentAddressRegistry(zone, registry); err != nil {
		return componentAddressRegistry{}, err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return componentAddressRegistry{}, errs.New(errs.KindValidationFailed, "Component id is invalid")
	}
	prefix, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || prefix.String() != zone.Desired.Subnet {
		return componentAddressRegistry{}, errs.New(errs.KindValidationFailed, "Zone subnet is invalid")
	}
	address, err := netip.ParseAddr(rawAddress)
	if err != nil || address.String() != rawAddress || ipam.ValidateUsableIPv4(prefix, address) != nil {
		return componentAddressRegistry{}, errs.New(errs.KindValidationFailed, "Component address is invalid")
	}
	if existing, found := registry.Reservations[componentID]; found {
		if existing != rawAddress {
			return componentAddressRegistry{}, errs.New(
				errs.KindStateConflict,
				"Component already has another address reservation",
			)
		}
		return cloneComponentAddressRegistry(registry), nil
	}
	for ownerID, reserved := range registry.Reservations {
		if reserved == rawAddress && ownerID != componentID {
			return componentAddressRegistry{}, errs.New(
				errs.KindStateConflict,
				"Component address is reserved by another Component",
			)
		}
	}
	next := cloneComponentAddressRegistry(registry)
	next.Reservations[componentID] = rawAddress
	return next, nil
}

func (registry componentAddressRegistry) release(
	zone zonerecord.Record,
	componentID string,
) (componentAddressRegistry, string, bool, error) {
	if err := validateComponentAddressRegistry(zone, registry); err != nil {
		return componentAddressRegistry{}, "", false, err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return componentAddressRegistry{}, "", false, errs.New(errs.KindValidationFailed, "Component id is invalid")
	}
	next := cloneComponentAddressRegistry(registry)
	address, found := next.Reservations[componentID]
	if found {
		delete(next.Reservations, componentID)
	}
	return next, address, found, nil
}

func (registry componentAddressRegistry) addresses(prefix netip.Prefix) ([]netip.Addr, error) {
	addresses := make([]netip.Addr, 0, len(registry.Reservations))
	for componentID, raw := range registry.Reservations {
		if err := ids.Validate(ids.KindComponent, componentID); err != nil {
			return nil, corruptComponentAddressRegistry()
		}
		address, err := netip.ParseAddr(raw)
		if err != nil || address.String() != raw || ipam.ValidateUsableIPv4(prefix, address) != nil {
			return nil, corruptComponentAddressRegistry()
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

func validateComponentAddressRegistry(zone zonerecord.Record, registry componentAddressRegistry) error {
	if err := zonerecord.ValidateRecord(zone); err != nil {
		return err
	}
	prefix, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil || prefix.String() != zone.Desired.Subnet {
		return corruptComponentAddressRegistry()
	}
	addresses, err := registry.addresses(prefix)
	if err != nil {
		return err
	}
	seen := make(map[netip.Addr]struct{}, len(addresses))
	for _, address := range addresses {
		if _, duplicate := seen[address]; duplicate {
			return corruptComponentAddressRegistry()
		}
		seen[address] = struct{}{}
	}
	return nil
}

func encodeComponentAddressRegistry(zone zonerecord.Record, registry componentAddressRegistry) ([]byte, error) {
	if err := validateComponentAddressRegistry(zone, registry); err != nil {
		return nil, err
	}
	return recordcodec.Encode("component_address_registry", registry)
}

func cloneComponentAddressRegistry(registry componentAddressRegistry) componentAddressRegistry {
	clone := componentAddressRegistry{Reservations: make(map[string]string, len(registry.Reservations))}
	for componentID, address := range registry.Reservations {
		clone.Reservations[componentID] = address
	}
	return clone
}

func corruptComponentAddressRegistry() error {
	return errs.New(errs.KindInternal, "Component address registry is corrupt")
}
