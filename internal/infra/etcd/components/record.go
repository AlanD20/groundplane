package components

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/netip"
	"reflect"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	RecordPrefix  = "/v1/records/components/"
	WriteFenceKey = "/v1/indexes/components/write-fence"
)

// Record separates operator-authored desired state from generated
// identity, address, and health owned by the Controller runtime.
type Record struct {
	Desired DesiredRecord `json:"desired"`
	Runtime RuntimeRecord `json:"runtime"`
}

type DesiredRecord struct {
	ID      string               `json:"id"`
	Owner   core.ComponentOwner  `json:"owner"`
	OwnerID string               `json:"owner_id,omitempty"`
	Kind    core.ComponentKind   `json:"kind"`
	Enabled bool                 `json:"enabled"`
	Config  core.ComponentConfig `json:"config,omitempty"`
}

type RuntimeRecord struct {
	GeneratedServices []string `json:"generated_services,omitempty"`
	PinnedIPv4        string   `json:"pinned_ipv4,omitempty"`
	Healthy           bool     `json:"healthy"`
}

func NewRecord(component core.Component) (Record, error) {
	desired, err := componentDesiredRecord(component)
	if err != nil {
		return Record{}, err
	}
	record := Record{
		Desired: desired,
		Runtime: RuntimeRecord{
			GeneratedServices: append([]string(nil), component.GeneratedServices...),
			PinnedIPv4:        component.PinnedIPv4,
			Healthy:           component.Healthy,
		},
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

// ReplaceDesired preserves every Controller-owned runtime field.
func ReplaceDesired(record Record, desired core.Component) (Record, error) {
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	projected, err := ProjectRecord(record)
	if err != nil {
		return Record{}, err
	}
	if !reflect.DeepEqual(desired.GeneratedServices, projected.GeneratedServices) ||
		desired.PinnedIPv4 != projected.PinnedIPv4 || desired.Healthy != projected.Healthy {
		return Record{}, errs.New(
			errs.KindValidationFailed,
			"Component desired replacement changed Controller-owned runtime state",
		)
	}
	nextDesired, err := componentDesiredRecord(desired)
	if err != nil {
		return Record{}, err
	}
	if nextDesired.ID != record.Desired.ID || nextDesired.Owner != record.Desired.Owner ||
		nextDesired.OwnerID != record.Desired.OwnerID || nextDesired.Kind != record.Desired.Kind {
		return Record{}, errs.New(
			errs.KindValidationFailed,
			"Component desired replacement changed stable identity, owner, or kind",
		)
	}
	replacement := record
	replacement.Desired = nextDesired
	if err := ValidateRecord(replacement); err != nil {
		return Record{}, err
	}
	return replacement, nil
}

// SetRuntime changes only Controller-owned generated and observed
// state while retaining the exact operator-authored desired configuration.
func SetRuntime(
	record Record,
	generatedServices []string,
	pinnedIPv4 string,
	healthy bool,
) (Record, error) {
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	replacement := record
	replacement.Runtime = RuntimeRecord{
		GeneratedServices: append([]string(nil), generatedServices...),
		PinnedIPv4:        pinnedIPv4,
		Healthy:           healthy,
	}
	if err := ValidateRecord(replacement); err != nil {
		return Record{}, err
	}
	return replacement, nil
}

func ProjectRecord(record Record) (core.Component, error) {
	if err := ValidateRecord(record); err != nil {
		return core.Component{}, err
	}
	return core.Component{
		ID: record.Desired.ID, Owner: record.Desired.Owner, OwnerID: record.Desired.OwnerID,
		Kind: record.Desired.Kind, Enabled: record.Desired.Enabled,
		Config:            core.CloneComponentConfig(record.Desired.Config),
		GeneratedServices: append([]string(nil), record.Runtime.GeneratedServices...),
		PinnedIPv4:        record.Runtime.PinnedIPv4,
		Healthy:           record.Runtime.Healthy,
	}, nil
}

func componentDesiredRecord(component core.Component) (DesiredRecord, error) {
	return DesiredRecord{
		ID: component.ID, Owner: component.Owner, OwnerID: component.OwnerID,
		Kind: component.Kind, Enabled: component.Enabled,
		Config: core.CloneComponentConfig(component.Config),
	}, nil
}

func RecordKey(id string) string { return RecordPrefix + id }

func WriteFenceMutation(authorityID string) etcdstore.Mutation {
	return etcdstore.Mutation{Type: etcdstore.MutationPut, Key: WriteFenceKey, Value: []byte(authorityID)}
}

func EnvironmentOwnerPrefix(environmentID string) string {
	return "/v1/indexes/components/by-owner/environment/" + environmentID + "/"
}

func EnvironmentOwnerKey(environmentID string, componentID string) string {
	return EnvironmentOwnerPrefix(environmentID) + componentID
}

func EnvironmentKindKey(environmentID string, kind core.ComponentKind) string {
	return "/v1/indexes/components/by-kind/environment/" + environmentID + "/" + recordcodec.EncodeKeySegment(string(kind))
}

func ValidateRecord(record Record) error {
	if err := recordcodec.ValidateID(ids.KindComponent, record.Desired.ID); err != nil {
		return err
	}
	projected := core.Component{
		ID: record.Desired.ID, Owner: record.Desired.Owner, OwnerID: record.Desired.OwnerID,
		Kind: record.Desired.Kind, Enabled: record.Desired.Enabled,
		Config:            core.CloneComponentConfig(record.Desired.Config),
		GeneratedServices: record.Runtime.GeneratedServices,
		PinnedIPv4:        record.Runtime.PinnedIPv4,
		Healthy:           record.Runtime.Healthy,
	}
	if err := projected.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.Desired.Owner == core.ComponentOwnerEnvironment {
		if err := recordcodec.ValidateID(ids.KindEnvironment, record.Desired.OwnerID); err != nil {
			return err
		}
	}
	if _, err := SecretReferences(record); err != nil {
		return err
	}
	seenServices := make(map[string]struct{}, len(record.Runtime.GeneratedServices))
	for _, serviceID := range record.Runtime.GeneratedServices {
		if err := recordcodec.ValidateID(ids.KindService, serviceID); err != nil {
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
	return nil
}

func EncodeRecord(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("component", record)
}

func DecodeRecord(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "component")
	if err != nil {
		return Record{}, err
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func SecretReferences(record Record) ([]string, error) {
	references := record.Desired.Config.SecretReferences()
	for _, reference := range references {
		if ids.Validate(ids.KindSecret, reference) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Component Secret reference is invalid")
		}
	}
	return references, nil
}
