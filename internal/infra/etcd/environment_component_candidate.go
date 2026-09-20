package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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
	Intent                    ComponentTaskIntent
	managedRuntimeSources     []ManagedComponentRuntimeSource
	appliedComponentRuntime   []byte
	appliedProjectionPresent  bool
	appliedProjectionRevision int64
	desiredProjectionRevision int64
	addresses                 []componentTaskAddressPreparation
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
	zoneChanges []EnvironmentBlueprintZoneChange,
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
	preparedZones, err := componentCandidateZones(environmentID, zones, zoneChanges)
	if err != nil {
		return ComponentTaskPreparation{}, err
	}
	fixedRevision := ordered[0].Current.ReadRevision
	if fixedRevision <= 0 {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"Component candidate Blueprint fence is invalid",
		)
	}
	for _, input := range ordered[1:] {
		if input.Current.ReadRevision != fixedRevision {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindValidationFailed,
				"Component candidates do not share one Blueprint fence",
			)
		}
	}
	appliedState, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentComposeProjectionKey(environmentID)}, Revision: fixedRevision,
	})
	if err != nil {
		return ComponentTaskPreparation{}, err
	}
	if appliedState == nil || appliedState.ReadRevision != fixedRevision || len(appliedState.Values) != 1 {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindInternal,
			"Component candidate applied projection read is incomplete",
		)
	}
	appliedValue := appliedState.Values[0]
	found := appliedValue != nil
	selected := Versioned[EnvironmentComposeProjection]{ReadRevision: fixedRevision}
	if found {
		projection, decodeErr := decodeEnvironmentComposeProjection(appliedValue.Value)
		if decodeErr != nil {
			return ComponentTaskPreparation{}, decodeErr
		}
		if projection.EnvironmentID != environmentID {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindStateConflict,
				"Component candidate applied Environment projection changed",
			)
		}
		selected.Record = projection
		selected.Revision = appliedValue.ModRevision
	}
	selectedZones := make(map[string]Versioned[ZoneRecord])
	if found {
		selectedZones = make(map[string]Versioned[ZoneRecord], len(selected.Record.DesiredZones))
		for _, desired := range selected.Record.DesiredZones {
			zone, joinErr := joinEnvironmentZone(selected, desired)
			if joinErr != nil {
				return ComponentTaskPreparation{}, joinErr
			}
			selectedZones[zone.Record.Desired.ID] = zone
		}
	}
	desired, _, err := currentEnvironmentProjectionAtRevision(ctx, repository.store, environmentID, fixedRevision)
	if err != nil {
		return ComponentTaskPreparation{}, err
	}
	managedRuntimeSources, err := selectManagedComponentRuntimeSources(desired.Record, selected.Record, found)
	if err != nil {
		return ComponentTaskPreparation{}, err
	}
	desiredZones := make(map[string]Versioned[ZoneRecord], len(desired.Record.DesiredZones))
	for _, item := range desired.Record.DesiredZones {
		zone, joinErr := joinEnvironmentZone(desired, item)
		if joinErr != nil {
			return ComponentTaskPreparation{}, joinErr
		}
		desiredZones[zone.Record.Desired.ID] = zone
	}
	for zoneID, change := range preparedZones {
		zone, selectedZone := selectedZones[zoneID]
		if change.Current == nil {
			if selectedZone {
				return ComponentTaskPreparation{}, errs.New(
					errs.KindStateConflict,
					"new Component candidate Zone is already selected",
				)
			}
			continue
		}
		if !found {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindStateConflict,
				"Component candidate Environment projection is unavailable",
			)
		}
		current, desiredZone := desiredZones[zoneID]
		if !desiredZone || change.Current.Revision != current.Revision ||
			!reflect.DeepEqual(change.Current.Record, current.Record) {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindStateConflict,
				"Component candidate desired Zone changed",
			)
		}
		// The selected projection is runtime placement evidence. Its Zone
		// topology may be stale while the desired Zone identity remains the
		// same; the desired full record and revision above are the mutation
		// authority and still fence concurrent edits.
		if !componentCandidateSelectedZoneIdentityMatches(change.Current.Record, zone.Record, selectedZone) {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindStateConflict,
				"Component candidate Zone changed in the selected projection",
			)
		}
	}

	keys := make([]string, 0, 1+len(ordered)+len(zones))
	keys = append(keys, componentTaskActiveEnvironmentKey(environmentID))
	for _, input := range ordered {
		keys = append(keys, componentKey(input.Current.Record.Desired.ID))
	}
	for _, zoneID := range zones {
		keys = append(keys, componentAddressRegistryKey(zoneID))
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
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
	registryOffset := 1 + len(ordered)
	for index, zoneID := range zones {
		zoneChange := preparedZones[zoneID]
		zoneRevision := int64(0)
		if zoneChange.Current != nil {
			zoneRevision = zoneChange.Current.Revision
		}
		registryValue := state.Values[registryOffset+index]
		currentRegistry := componentAddressRegistry{Reservations: map[string]string{}}
		registryRevision := int64(0)
		if zoneChange.Current == nil && registryValue != nil {
			return ComponentTaskPreparation{}, corruptComponentAddressRegistry()
		}
		if registryValue != nil {
			decoded, decodeErr := decodeEnvelope[componentAddressRegistry](
				registryValue.Value,
				"component_address_registry",
			)
			if decodeErr != nil || validateComponentAddressRegistry(zoneChange.Record, decoded) != nil {
				return ComponentTaskPreparation{}, corruptComponentAddressRegistry()
			}
			currentRegistry = decoded
			registryRevision = registryValue.ModRevision
		}
		registries[zoneID] = cloneComponentAddressRegistry(currentRegistry)
		addresses[index] = componentTaskAddressPreparation{
			Zone: Versioned[ZoneRecord]{
				Record: zoneChange.Record, Revision: zoneRevision, ReadRevision: fixedRevision,
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
	preparation := ComponentTaskPreparation{
		Intent:                    intent,
		managedRuntimeSources:     managedRuntimeSources,
		appliedComponentRuntime:   append([]byte(nil), selected.Record.ComposeArtifact...),
		appliedProjectionPresent:  found,
		appliedProjectionRevision: keyValueRevision(appliedValue),
		desiredProjectionRevision: desired.Revision,
		addresses:                 addresses,
	}
	if err := validateComponentTaskPreparation(preparation); err != nil {
		return ComponentTaskPreparation{}, err
	}
	return cloneComponentTaskPreparation(preparation), nil
}

func componentCandidateSelectedZoneIdentityMatches(
	current ZoneRecord,
	selected ZoneRecord,
	selectedPresent bool,
) bool {
	if !selectedPresent {
		return true
	}
	return current.EnvironmentID == selected.EnvironmentID && current.Desired.ID == selected.Desired.ID
}

func componentCandidateZones(
	environmentID string,
	wanted []string,
	changes []EnvironmentBlueprintZoneChange,
) (map[string]EnvironmentBlueprintZoneChange, error) {
	wantedSet := make(map[string]struct{}, len(wanted))
	for _, zoneID := range wanted {
		wantedSet[zoneID] = struct{}{}
	}
	result := make(map[string]EnvironmentBlueprintZoneChange, len(wanted))
	for _, change := range changes {
		zoneID := change.Record.Desired.ID
		if _, needed := wantedSet[zoneID]; !needed {
			continue
		}
		if _, duplicate := result[zoneID]; duplicate || validateZoneRecord(change.Record) != nil ||
			change.Record.EnvironmentID != environmentID {
			return nil, errs.New(errs.KindValidationFailed, "Component candidate Zone input is invalid")
		}
		if change.Current != nil &&
			(change.Current.Revision <= 0 || change.Current.ReadRevision < change.Current.Revision ||
				change.Current.Record.EnvironmentID != environmentID || change.Current.Record.Desired.ID != zoneID) {
			return nil, errs.New(errs.KindValidationFailed, "Component candidate Zone version is invalid")
		}
		result[zoneID] = change
	}
	if len(result) != len(wanted) {
		return nil, errs.New(errs.KindValidationFailed, "Component candidate Zone is not in the Blueprint")
	}
	return result, nil
}

func projectedComponentCandidateZone(component core.Component) (string, bool, error) {
	if component.Kind != core.ComponentKindIngressCaddy || !component.Enabled {
		return "", false, nil
	}
	if component.Config.Caddy == nil || len(component.Config.Caddy.ZoneIDs) == 0 ||
		ids.Validate(ids.KindNetwork, component.Config.Caddy.ZoneIDs[0]) != nil {
		return "", false, errs.New(
			errs.KindValidationFailed,
			"enabled Caddy Component candidate has an invalid Zone",
		)
	}
	return component.Config.Caddy.ZoneIDs[0], true, nil
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
	if len(preparation.managedRuntimeSources) > maximumManagedComponentRuntimeSources {
		return errs.New(errs.KindValidationFailed, "Component candidate managed runtime source count is invalid")
	}
	if preparation.desiredProjectionRevision < 0 || preparation.appliedProjectionRevision < 0 ||
		preparation.appliedProjectionPresent != (preparation.appliedProjectionRevision > 0) {
		return errs.New(errs.KindValidationFailed, "Component candidate applied projection evidence is invalid")
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
		if zoneID <= previousZoneID || address.Zone.Revision < 0 ||
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
		Intent:                    cloneComponentTaskIntent(preparation.Intent),
		managedRuntimeSources:     append([]ManagedComponentRuntimeSource(nil), preparation.managedRuntimeSources...),
		appliedComponentRuntime:   append([]byte(nil), preparation.appliedComponentRuntime...),
		appliedProjectionPresent:  preparation.appliedProjectionPresent,
		appliedProjectionRevision: preparation.appliedProjectionRevision,
		desiredProjectionRevision: preparation.desiredProjectionRevision,
		addresses:                 make([]componentTaskAddressPreparation, len(preparation.addresses)),
	}
	for index, address := range preparation.addresses {
		clone.addresses[index] = address
		clone.addresses[index].Current.Record = cloneComponentAddressRegistry(address.Current.Record)
		clone.addresses[index].Next = cloneComponentAddressRegistry(address.Next)
	}
	return clone
}
