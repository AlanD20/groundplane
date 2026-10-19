package componentwork

import (
	"context"
	"sort"

	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

// Address settlement touches only this operation's captured reservations. Other
// Components and Service proxy reservations survive unchanged.
func (repository *InputRepository) settleAddresses(ctx context.Context, view workqueue.View,
	capture Capture, keep componentrecord.Record,
) (change workqueue.Change, err error) {
	defer func() {
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
		}
	}()
	retained, hasRetained, err := environmentchanges.ComponentAddress(keep)
	if err != nil {
		return change, err
	}
	expected := map[string]string{}
	for _, record := range []componentrecord.Record{capture.Current, capture.Candidate} {
		binding, present, err := environmentchanges.ComponentAddress(record)
		if err != nil {
			return change, err
		}
		if !present {
			continue
		}
		if prior, found := expected[binding.ZoneID()]; found && prior != binding.Address() {
			return change, conflict("Component captured two different reservations in one Zone")
		}
		expected[binding.ZoneID()] = binding.Address()
	}
	zones := map[string]zonerecord.Record{}
	if capture.Predecessor != nil {
		for _, zone := range capture.Predecessor.Zones {
			zones[zone.Desired.ID] = zone
		}
	}
	for _, zone := range capture.Zones {
		zones[zone.Desired.ID] = zone
	}
	if hasRetained && expected[retained.ZoneID()] != retained.Address() {
		return change, conflict("Restored Component address was not reserved by this operation")
	}
	ordered := make([]string, 0, len(expected))
	for id := range expected {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	for _, id := range ordered {
		zone, found := zones[id]
		if !found {
			return change, conflict("Component reservation has no captured Zone")
		}
		key := networkreservations.ComponentAddressRegistryKey(id)
		read, err := repository.read(ctx, []string{key}, view.ReadRevision)
		if err != nil {
			return change, err
		}
		if read.Values[0] == nil {
			return change, conflict("Component reservation disappeared before settlement")
		}
		value := read.Values[0]
		registry, decodeErr := recordcodec.Decode[networkreservations.ComponentAddressRegistry](
			value.Value,
			"component_address_registry",
		)
		revision := value.ModRevision
		keyvalue.ClearValues(read.Values)
		if decodeErr != nil || networkreservations.ValidateComponentAddressRegistry(zone, registry) != nil ||
			registry.Reservations[view.Item.Item.TargetID] != expected[id] {
			return change, conflict("Component reservation changed before settlement")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: revision})
		if hasRetained && retained.ZoneID() == id {
			continue
		}
		delete(registry.Reservations, view.Item.Item.TargetID)
		encoded, err := networkreservations.EncodeComponentAddressRegistry(zone, registry)
		if err != nil {
			return change, err
		}
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded},
		)
	}
	return change, nil
}
