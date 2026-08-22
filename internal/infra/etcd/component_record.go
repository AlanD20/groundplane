package etcd

import (
	"bytes"
	"encoding/json"
	"net/netip"
	"reflect"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const componentPrefix = "/v1/records/components/"

// ComponentRecord separates operator-authored desired state from generated
// identity, address, and health owned by the Controller runtime.
type ComponentRecord struct {
	Desired ComponentDesiredRecord `json:"desired"`
	Runtime ComponentRuntimeRecord `json:"runtime"`
}

type ComponentDesiredRecord struct {
	ID      string                     `json:"id"`
	Owner   core.ComponentOwner        `json:"owner"`
	OwnerID string                     `json:"owner_id,omitempty"`
	Kind    core.ComponentKind         `json:"kind"`
	Enabled bool                       `json:"enabled"`
	Config  map[string]json.RawMessage `json:"config,omitempty"`
}

type ComponentRuntimeRecord struct {
	GeneratedServices []string `json:"generated_services,omitempty"`
	PinnedIPv4        string   `json:"pinned_ipv4,omitempty"`
	Healthy           bool     `json:"healthy"`
}

func NewComponentRecord(component core.Component) (ComponentRecord, error) {
	desired, err := componentDesiredRecord(component)
	if err != nil {
		return ComponentRecord{}, err
	}
	record := ComponentRecord{
		Desired: desired,
		Runtime: ComponentRuntimeRecord{
			GeneratedServices: append([]string(nil), component.GeneratedServices...),
			PinnedIPv4:        component.PinnedIPv4,
			Healthy:           component.Healthy,
		},
	}
	if err := validateComponentRecord(record); err != nil {
		return ComponentRecord{}, err
	}
	return record, nil
}

// ReplaceComponentDesired preserves every Controller-owned runtime field.
func ReplaceComponentDesired(record ComponentRecord, desired core.Component) (ComponentRecord, error) {
	if err := validateComponentRecord(record); err != nil {
		return ComponentRecord{}, err
	}
	projected, err := ProjectComponentRecord(record)
	if err != nil {
		return ComponentRecord{}, err
	}
	if !reflect.DeepEqual(desired.GeneratedServices, projected.GeneratedServices) ||
		desired.PinnedIPv4 != projected.PinnedIPv4 || desired.Healthy != projected.Healthy {
		return ComponentRecord{}, errs.New(
			errs.KindValidationFailed,
			"Component desired replacement changed Controller-owned runtime state",
		)
	}
	nextDesired, err := componentDesiredRecord(desired)
	if err != nil {
		return ComponentRecord{}, err
	}
	if nextDesired.ID != record.Desired.ID || nextDesired.Owner != record.Desired.Owner ||
		nextDesired.OwnerID != record.Desired.OwnerID || nextDesired.Kind != record.Desired.Kind {
		return ComponentRecord{}, errs.New(
			errs.KindValidationFailed,
			"Component desired replacement changed stable identity, owner, or kind",
		)
	}
	replacement := record
	replacement.Desired = nextDesired
	if err := validateComponentRecord(replacement); err != nil {
		return ComponentRecord{}, err
	}
	return replacement, nil
}

// SetComponentRuntime changes only Controller-owned generated and observed
// state while retaining the exact operator-authored desired configuration.
func SetComponentRuntime(
	record ComponentRecord,
	generatedServices []string,
	pinnedIPv4 string,
	healthy bool,
) (ComponentRecord, error) {
	if err := validateComponentRecord(record); err != nil {
		return ComponentRecord{}, err
	}
	replacement := record
	replacement.Runtime = ComponentRuntimeRecord{
		GeneratedServices: append([]string(nil), generatedServices...),
		PinnedIPv4:        pinnedIPv4,
		Healthy:           healthy,
	}
	if err := validateComponentRecord(replacement); err != nil {
		return ComponentRecord{}, err
	}
	return replacement, nil
}

func ProjectComponentRecord(record ComponentRecord) (core.Component, error) {
	if err := validateComponentRecord(record); err != nil {
		return core.Component{}, err
	}
	config := make(map[string]any, len(record.Desired.Config))
	for key, raw := range record.Desired.Config {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return core.Component{}, corruptRecord()
		}
		config[key] = value
	}
	if len(config) == 0 {
		config = nil
	}
	return core.Component{
		ID: record.Desired.ID, Owner: record.Desired.Owner, OwnerID: record.Desired.OwnerID,
		Kind: record.Desired.Kind, Enabled: record.Desired.Enabled, Config: config,
		GeneratedServices: append([]string(nil), record.Runtime.GeneratedServices...),
		PinnedIPv4:        record.Runtime.PinnedIPv4,
		Healthy:           record.Runtime.Healthy,
	}, nil
}

func componentDesiredRecord(component core.Component) (ComponentDesiredRecord, error) {
	config := make(map[string]json.RawMessage, len(component.Config))
	for key, value := range component.Config {
		if strings.TrimSpace(key) == "" {
			return ComponentDesiredRecord{}, errs.New(errs.KindValidationFailed, "Component config key is required")
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return ComponentDesiredRecord{}, errs.New(
				errs.KindValidationFailed,
				"Component config must be JSON-compatible",
			)
		}
		config[key] = raw
	}
	if len(config) == 0 {
		config = nil
	}
	return ComponentDesiredRecord{
		ID: component.ID, Owner: component.Owner, OwnerID: component.OwnerID,
		Kind: component.Kind, Enabled: component.Enabled, Config: config,
	}, nil
}

func componentKey(id string) string { return componentPrefix + id }

func componentEnvironmentOwnerPrefix(environmentID string) string {
	return "/v1/indexes/components/by-owner/environment/" + environmentID + "/"
}

func componentEnvironmentOwnerKey(environmentID string, componentID string) string {
	return componentEnvironmentOwnerPrefix(environmentID) + componentID
}

func componentEnvironmentKindKey(environmentID string, kind core.ComponentKind) string {
	return "/v1/indexes/components/by-kind/environment/" + environmentID + "/" + encodeDynamicSegment(string(kind))
}

func validateComponentRecord(record ComponentRecord) error {
	if err := validateID(ids.KindComponent, record.Desired.ID); err != nil {
		return err
	}
	projected := core.Component{
		ID: record.Desired.ID, Owner: record.Desired.Owner, OwnerID: record.Desired.OwnerID,
		Kind: record.Desired.Kind, Enabled: record.Desired.Enabled,
		GeneratedServices: record.Runtime.GeneratedServices,
		PinnedIPv4:        record.Runtime.PinnedIPv4,
		Healthy:           record.Runtime.Healthy,
	}
	if err := projected.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.Desired.Owner == core.ComponentOwnerEnvironment {
		if err := validateID(ids.KindEnvironment, record.Desired.OwnerID); err != nil {
			return err
		}
	}
	seenServices := make(map[string]struct{}, len(record.Runtime.GeneratedServices))
	for _, serviceID := range record.Runtime.GeneratedServices {
		if err := validateID(ids.KindService, serviceID); err != nil {
			return err
		}
		if _, duplicate := seenServices[serviceID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Component generated Service ids must be unique")
		}
		seenServices[serviceID] = struct{}{}
	}
	if record.Runtime.PinnedIPv4 != "" {
		address, err := netip.ParseAddr(record.Runtime.PinnedIPv4)
		if err != nil || !address.Is4() || address.String() != record.Runtime.PinnedIPv4 {
			return errs.New(errs.KindValidationFailed, "Component pinned IPv4 must be canonical")
		}
	}
	for key, raw := range record.Desired.Config {
		if strings.TrimSpace(key) == "" || !json.Valid(raw) {
			return errs.New(errs.KindValidationFailed, "Component config is invalid")
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return errs.New(errs.KindValidationFailed, "Component config is invalid")
		}
		canonical, err := json.Marshal(value)
		if err != nil || !bytes.Equal(raw, canonical) {
			return errs.New(errs.KindValidationFailed, "Component config is not canonical")
		}
	}
	return nil
}

func encodeComponentRecord(record ComponentRecord) ([]byte, error) {
	if err := validateComponentRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("component", record)
}

func decodeComponentRecord(value []byte) (ComponentRecord, error) {
	record, err := decodeEnvelope[ComponentRecord](value, "component")
	if err != nil {
		return ComponentRecord{}, err
	}
	if err := validateComponentRecord(record); err != nil {
		return ComponentRecord{}, corruptRecord()
	}
	return record, nil
}
