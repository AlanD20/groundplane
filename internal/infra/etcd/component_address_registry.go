package etcd

import (
	"context"
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
	zone ZoneRecord,
) (Versioned[componentAddressRegistry], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[componentAddressRegistry]{}, err
	}
	if err := validateZoneRecord(zone); err != nil {
		return Versioned[componentAddressRegistry]{}, err
	}
	result, err := store.Get(ctx, componentAddressRegistryKey(zone.Desired.ID))
	if err != nil {
		return Versioned[componentAddressRegistry]{}, err
	}
	if result.Entry == nil {
		return Versioned[componentAddressRegistry]{
			Record:       componentAddressRegistry{Reservations: map[string]string{}},
			ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := decodeEnvelope[componentAddressRegistry](result.Entry.Value, "component_address_registry")
	if err != nil || validateComponentAddressRegistry(zone, registry) != nil {
		return Versioned[componentAddressRegistry]{}, corruptComponentAddressRegistry()
	}
	return Versioned[componentAddressRegistry]{
		Record: registry, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (registry componentAddressRegistry) reserve(
	zone ZoneRecord,
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
	address, err := ipam.FirstAvailableUsableIPv4(prefix, reserved)
	if err != nil {
		return componentAddressRegistry{}, "", err
	}
	next := cloneComponentAddressRegistry(registry)
	next.Reservations[componentID] = address.String()
	return next, address.String(), nil
}

func (registry componentAddressRegistry) release(
	zone ZoneRecord,
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

func validateComponentAddressRegistry(zone ZoneRecord, registry componentAddressRegistry) error {
	if err := validateZoneRecord(zone); err != nil {
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

func encodeComponentAddressRegistry(zone ZoneRecord, registry componentAddressRegistry) ([]byte, error) {
	if err := validateComponentAddressRegistry(zone, registry); err != nil {
		return nil, err
	}
	return encodeEnvelope("component_address_registry", registry)
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
