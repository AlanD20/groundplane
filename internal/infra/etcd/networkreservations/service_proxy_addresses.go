package networkreservations

import (
	"context"
	"maps"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProxyAddresses is prepared before rendering, then committed with the Release
// Task. No reservation becomes visible merely because a candidate was prepared.
type ProxyAddresses struct {
	addresses  map[string]map[string]string
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
}

func (p ProxyAddresses) ForService(id string) map[string]string { return maps.Clone(p.addresses[id]) }
func (p ProxyAddresses) Conditions() []keyvalue.Condition       { return slices.Clone(p.conditions) }
func (p ProxyAddresses) Mutations() []keyvalue.Mutation         { return p.mutations }
func (p ProxyAddresses) Clear()                                 { keyvalue.ZeroMutationBytes(p.mutations) }

func (registry ComponentAddressRegistry) ReserveService(
	zone zones.Record,
	serviceID string,
) (ComponentAddressRegistry, string, error) {
	if err := ValidateComponentAddressRegistry(zone, registry); err != nil {
		return ComponentAddressRegistry{}, "", err
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return ComponentAddressRegistry{}, "", errs.New(errs.KindValidationFailed, "Service id is invalid")
	}
	if address, found := registry.ServiceReservations[serviceID]; found {
		return CloneComponentAddressRegistry(registry), address, nil
	}
	prefix, err := ipam.ParseIPv4Prefix(zone.Desired.Subnet)
	if err != nil {
		return ComponentAddressRegistry{}, "", err
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
	if next.ServiceReservations == nil {
		next.ServiceReservations = map[string]string{}
	}
	next.ServiceReservations[serviceID] = address.String()
	return next, address.String(), nil
}

// The artifact uses names from the captured Zone snapshot. Reservations belong
// to stable ids; Components and proxies share one collision-checked allocator.
func (planner *Planner) PrepareProxyAddresses(
	ctx context.Context,
	zoneInputs []zones.Record,
	services []core.Service,
) (ProxyAddresses, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return ProxyAddresses{}, err
	}
	result := ProxyAddresses{addresses: map[string]map[string]string{}}
	byID := map[string]zones.Record{}
	byName := map[string]string{}
	for _, zone := range zoneInputs {
		byID[zone.Desired.ID] = zone
		byName[zone.Desired.Name] = zone.Desired.ID
	}
	selected := slices.Clone(services)
	sort.Slice(selected, func(i, j int) bool { return selected[i].ID < selected[j].ID })
	owners := map[string][]core.Service{}
	for _, service := range selected {
		if len(service.Expose) == 0 || service.Adapter != "" {
			continue
		}
		for _, name := range service.Zones {
			id, exists := byName[name]
			if !exists {
				return ProxyAddresses{}, errs.New(
					errs.KindStateConflict,
					"Service proxy Zone is absent from its projection",
				)
			}
			owners[id] = append(owners[id], service)
		}
	}
	zoneIDs := make([]string, 0, len(owners))
	for id := range owners {
		zoneIDs = append(zoneIDs, id)
	}
	sort.Strings(zoneIDs)
	keys := make([]string, len(zoneIDs))
	for i, id := range zoneIDs {
		keys[i] = ComponentAddressRegistryKey(id)
	}
	if len(keys) == 0 {
		return result, nil
	}
	read, err := planner.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return ProxyAddresses{}, err
	}
	if read == nil || len(read.Values) != len(keys) {
		return ProxyAddresses{}, errs.New(errs.KindInternal, "proxy address registry read is incomplete")
	}
	for i, id := range zoneIDs {
		zone := byID[id]
		registry := ComponentAddressRegistry{Reservations: map[string]string{}}
		condition := keyvalue.Condition{Key: keys[i]}
		if value := read.Values[i]; value != nil {
			registry, err = recordcodec.Decode[ComponentAddressRegistry](value.Value, "component_address_registry")
			if err != nil || ValidateComponentAddressRegistry(zone, registry) != nil {
				result.Clear()
				return ProxyAddresses{}, CorruptComponentAddressRegistry()
			}
			condition.ModRevision = value.ModRevision
		}
		next := registry
		for _, service := range owners[id] {
			var address string
			next, address, err = next.ReserveService(zone, service.ID)
			if err != nil {
				result.Clear()
				return ProxyAddresses{}, err
			}
			if result.addresses[service.ID] == nil {
				result.addresses[service.ID] = map[string]string{}
			}
			result.addresses[service.ID][zone.Desired.Name] = address
		}
		result.conditions = append(result.conditions, condition)
		if !EqualComponentAddressRegistry(registry, next) {
			value, err := EncodeComponentAddressRegistry(zone, next)
			if err != nil {
				result.Clear()
				return ProxyAddresses{}, err
			}
			result.mutations = append(
				result.mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[i], Value: value},
			)
		}
	}
	return result, nil
}
