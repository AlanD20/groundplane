package etcd

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentComponentCandidateInput combines one fixed active Component with
// the complete Controller-projected replacement. PinnedIPv4 and Healthy are
// deliberately rejected on Candidate because preparation owns address
// allocation and health remains observed state.
type EnvironmentComponentCandidateInput struct {
	Current   Versioned[ComponentRecord]
	Candidate core.Component
}

// ComponentTaskPreparation is an immutable, side-effect-free candidate plan.
// ApplyEnvironmentBlueprintWithTask consumes its private CAS evidence in the
// same transaction that publishes its Intent and Task.
type ComponentTaskPreparation struct {
	Intent    ComponentTaskIntent
	addresses []componentTaskAddressPreparation
}

type componentTaskAddressPreparation struct {
	Zone    Versioned[ZoneRecord]
	Current Versioned[componentAddressRegistry]
	Next    componentAddressRegistry
	Mutates bool
}

// PrepareEnvironmentComponentTask computes candidate records and address
// reservations without changing durable state. A caller may render from the
// returned Intent, but only the later atomic Blueprint transaction may publish
// it.
func (repository *HierarchyRepository) PrepareEnvironmentComponentTask(
	ctx context.Context,
	taskID string,
	environmentID string,
	inputs []EnvironmentComponentCandidateInput,
	createdAt time.Time,
) (ComponentTaskPreparation, error) {
	if err := validateContext(ctx); err != nil {
		return ComponentTaskPreparation{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"Component candidate identity is invalid",
		)
	}
	if err := validateTimestamp("Component candidate created_at", createdAt); err != nil {
		return ComponentTaskPreparation{}, err
	}
	if len(inputs) == 0 || len(inputs) > 2 {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"Component candidate input count is invalid",
		)
	}
	ordered := append([]EnvironmentComponentCandidateInput(nil), inputs...)
	sort.Slice(ordered, func(left int, right int) bool {
		return ordered[left].Current.Record.Desired.ID < ordered[right].Current.Record.Desired.ID
	})
	zoneSet := make(map[string]struct{})
	previousID := ""
	for _, input := range ordered {
		if err := validateComponentVersion(input.Current); err != nil {
			return ComponentTaskPreparation{}, err
		}
		current := input.Current.Record.Desired
		candidate := input.Candidate
		if current.ID == previousID || candidate.ID != current.ID || candidate.Owner != current.Owner ||
			candidate.OwnerID != current.OwnerID || candidate.Kind != current.Kind ||
			candidate.Owner != core.ComponentOwnerEnvironment || candidate.OwnerID != environmentID {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindValidationFailed,
				"Component candidate changed stable ownership",
			)
		}
		previousID = current.ID
		if candidate.PinnedIPv4 != "" || candidate.Healthy {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindValidationFailed,
				"Component candidate supplied Controller-owned address or health state",
			)
		}
		currentBinding, currentPresent, err := componentTaskAddress(input.Current.Record)
		if err != nil {
			return ComponentTaskPreparation{}, err
		}
		if currentPresent {
			zoneSet[currentBinding.zoneID] = struct{}{}
		}
		candidateZoneID, candidatePresent, err := projectedComponentCandidateZone(candidate)
		if err != nil {
			return ComponentTaskPreparation{}, err
		}
		if candidatePresent {
			zoneSet[candidateZoneID] = struct{}{}
		}
	}
	zones := make([]string, 0, len(zoneSet))
	for zoneID := range zoneSet {
		zones = append(zones, zoneID)
	}
	sort.Strings(zones)

	keys := make([]string, 0, 1+len(ordered)+(2*len(zones)))
	keys = append(keys, componentTaskActiveEnvironmentKey(environmentID))
	for _, input := range ordered {
		keys = append(keys, componentKey(input.Current.Record.Desired.ID))
	}
	for _, zoneID := range zones {
		keys = append(keys, zoneKey(zoneID))
	}
	for _, zoneID := range zones {
		keys = append(keys, componentAddressRegistryKey(zoneID))
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return ComponentTaskPreparation{}, err
	}
	if state == nil || len(state.Values) != len(keys) {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindInternal,
			"Component candidate preparation read is incomplete",
		)
	}
	if state.Values[0] != nil {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindStateConflict,
			"Environment already has an active Component reconciliation",
		)
	}
	for index, input := range ordered {
		value := state.Values[index+1]
		if value == nil || value.ModRevision != input.Current.Revision {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindStateConflict,
				"active Component changed during preparation",
			)
		}
		stored, decodeErr := decodeComponentRecord(value.Value)
		if decodeErr != nil {
			return ComponentTaskPreparation{}, decodeErr
		}
		if !reflect.DeepEqual(stored, input.Current.Record) {
			return ComponentTaskPreparation{}, errs.New(errs.KindStateConflict, "active Component snapshot changed")
		}
	}

	addresses := make([]componentTaskAddressPreparation, len(zones))
	registries := make(map[string]componentAddressRegistry, len(zones))
	zoneOffset := 1 + len(ordered)
	registryOffset := zoneOffset + len(zones)
	for index, zoneID := range zones {
		zoneValue := state.Values[zoneOffset+index]
		if zoneValue == nil {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindStateConflict,
				"Component candidate Zone was not found",
			)
		}
		zone, decodeErr := decodeZoneRecord(zoneValue.Value)
		if decodeErr != nil {
			return ComponentTaskPreparation{}, decodeErr
		}
		if zone.EnvironmentID != environmentID || zone.Desired.ID != zoneID {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindStateConflict,
				"Component candidate Zone ownership changed",
			)
		}
		registryValue := state.Values[registryOffset+index]
		currentRegistry := componentAddressRegistry{Reservations: map[string]string{}}
		registryRevision := int64(0)
		if registryValue != nil {
			currentRegistry, decodeErr = decodeEnvelope[componentAddressRegistry](
				registryValue.Value,
				"component_address_registry",
			)
			if decodeErr != nil || validateComponentAddressRegistry(zone, currentRegistry) != nil {
				return ComponentTaskPreparation{}, corruptComponentAddressRegistry()
			}
			registryRevision = registryValue.ModRevision
		}
		registries[zoneID] = cloneComponentAddressRegistry(currentRegistry)
		addresses[index] = componentTaskAddressPreparation{
			Zone: Versioned[ZoneRecord]{
				Record: zone, Revision: zoneValue.ModRevision, ReadRevision: state.ReadRevision,
			},
			Current: Versioned[componentAddressRegistry]{
				Record:       cloneComponentAddressRegistry(currentRegistry),
				Revision:     registryRevision,
				ReadRevision: state.ReadRevision,
			},
			Next: cloneComponentAddressRegistry(currentRegistry),
		}
	}
	if err := validatePreparedCurrentComponentReservations(ordered, registries); err != nil {
		return ComponentTaskPreparation{}, err
	}

	candidates := make([]ComponentTaskCandidate, len(ordered))
	for index, input := range ordered {
		projected := input.Candidate
		candidateZoneID, candidatePresent, _ := projectedComponentCandidateZone(projected)
		if candidatePresent {
			currentBinding, currentPresent, _ := componentTaskAddress(input.Current.Record)
			if currentPresent && currentBinding.zoneID == candidateZoneID {
				projected.PinnedIPv4 = currentBinding.address
			} else {
				addressIndex := sort.SearchStrings(zones, candidateZoneID)
				registry, address, reserveErr := addresses[addressIndex].Next.reserve(
					addresses[addressIndex].Zone.Record,
					input.Current.Record.Desired.ID,
				)
				if reserveErr != nil {
					return ComponentTaskPreparation{}, reserveErr
				}
				addresses[addressIndex].Next = registry
				addresses[addressIndex].Mutates = true
				projected.PinnedIPv4 = address
			}
		}
		projected.Healthy = false
		record, recordErr := NewComponentRecord(projected)
		if recordErr != nil {
			return ComponentTaskPreparation{}, recordErr
		}
		candidates[index] = ComponentTaskCandidate{
			CurrentRevision: input.Current.Revision,
			Current:         input.Current.Record,
			Candidate:       record,
		}
	}
	intent, err := NewComponentTaskIntent(taskID, environmentID, candidates, createdAt)
	if err != nil {
		return ComponentTaskPreparation{}, err
	}
	preparation := ComponentTaskPreparation{Intent: intent, addresses: addresses}
	if err := validateComponentTaskPreparation(preparation); err != nil {
		return ComponentTaskPreparation{}, err
	}
	return cloneComponentTaskPreparation(preparation), nil
}

func projectedComponentCandidateZone(component core.Component) (string, bool, error) {
	if component.Kind != core.ComponentKindIngressCaddy || !component.Enabled {
		return "", false, nil
	}
	raw, found := component.Config["zone_id"]
	zoneID, ok := raw.(string)
	if !found || !ok || ids.Validate(ids.KindNetwork, zoneID) != nil {
		return "", false, errs.New(
			errs.KindValidationFailed,
			"enabled Caddy Component candidate has an invalid Zone",
		)
	}
	return zoneID, true, nil
}

func validatePreparedCurrentComponentReservations(
	inputs []EnvironmentComponentCandidateInput,
	registries map[string]componentAddressRegistry,
) error {
	for _, input := range inputs {
		binding, present, err := componentTaskAddress(input.Current.Record)
		if err != nil {
			return err
		}
		if present && registries[binding.zoneID].Reservations[input.Current.Record.Desired.ID] != binding.address {
			return errs.New(errs.KindStateConflict, "active Component address reservation changed")
		}
	}
	return nil
}

func validateComponentTaskPreparation(preparation ComponentTaskPreparation) error {
	if err := validateComponentTaskIntent(preparation.Intent); err != nil {
		return err
	}
	wantZones := make(map[string]struct{})
	for _, candidate := range preparation.Intent.Candidates {
		for _, record := range []ComponentRecord{candidate.Current, candidate.Candidate} {
			binding, present, err := componentTaskAddress(record)
			if err != nil {
				return err
			}
			if present {
				wantZones[binding.zoneID] = struct{}{}
			}
		}
	}
	if len(preparation.addresses) != len(wantZones) {
		return errs.New(errs.KindValidationFailed, "Component candidate address evidence is incomplete")
	}
	registries := make(map[string]componentAddressRegistry, len(preparation.addresses))
	previousZoneID := ""
	for _, address := range preparation.addresses {
		zoneID := address.Zone.Record.Desired.ID
		if zoneID <= previousZoneID || address.Zone.Revision <= 0 ||
			address.Zone.ReadRevision < address.Zone.Revision || address.Current.Revision < 0 ||
			address.Current.ReadRevision < address.Current.Revision ||
			validateZoneRecord(address.Zone.Record) != nil ||
			validateComponentAddressRegistry(address.Zone.Record, address.Current.Record) != nil ||
			validateComponentAddressRegistry(address.Zone.Record, address.Next) != nil {
			return errs.New(errs.KindValidationFailed, "Component candidate address evidence is invalid")
		}
		if _, wanted := wantZones[zoneID]; !wanted ||
			address.Mutates == reflect.DeepEqual(address.Current.Record, address.Next) {
			return errs.New(errs.KindValidationFailed, "Component candidate address transition is invalid")
		}
		previousZoneID = zoneID
		registries[zoneID] = address.Next
	}
	return validateComponentTaskReservations(preparation.Intent, registries)
}

func cloneComponentTaskPreparation(preparation ComponentTaskPreparation) ComponentTaskPreparation {
	clone := ComponentTaskPreparation{
		Intent:    cloneComponentTaskIntent(preparation.Intent),
		addresses: make([]componentTaskAddressPreparation, len(preparation.addresses)),
	}
	for index, address := range preparation.addresses {
		clone.addresses[index] = address
		clone.addresses[index].Current.Record = cloneComponentAddressRegistry(address.Current.Record)
		clone.addresses[index].Next = cloneComponentAddressRegistry(address.Next)
	}
	return clone
}
