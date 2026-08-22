package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const servicePrefix = "/v1/records/services/"

// ServiceRecord is the atomic durable boundary for one Service. Desired and
// runtime remain distinct typed subrecords so Blueprint input cannot author or
// overwrite Controller-owned operational intent.
type ServiceRecord struct {
	EnvironmentID    string              `json:"environment_id"`
	BackingNetworkID string              `json:"backing_network_id,omitempty"`
	Desired          core.Service        `json:"desired"`
	Runtime          core.ServiceRuntime `json:"runtime"`
}

// NewServiceRecord constructs the only accepted initial runtime state for a
// Service first created directly or introduced by Blueprint.
func NewServiceRecord(environmentID string, desired core.Service, backingNetworkID string) (ServiceRecord, error) {
	record := ServiceRecord{
		EnvironmentID:    environmentID,
		BackingNetworkID: backingNetworkID,
		Desired:          desired,
		Runtime: core.ServiceRuntime{
			ServiceID:     desired.ID,
			RuntimeIntent: core.ServiceRuntimeIntentRunning,
		},
	}
	if err := validateServiceRecord(record); err != nil {
		return ServiceRecord{}, err
	}
	return record, nil
}

// ReplaceServiceDesired applies a Blueprint or direct edit without changing
// Controller-owned runtime intent.
func ReplaceServiceDesired(record ServiceRecord, desired core.Service) (ServiceRecord, error) {
	if err := validateServiceRecord(record); err != nil {
		return ServiceRecord{}, err
	}
	if desired.ID != record.Desired.ID {
		return ServiceRecord{}, errs.New(errs.KindValidationFailed, "Service desired replacement changed stable id")
	}
	replacement := record
	replacement.Desired = desired
	if err := validateServiceRecord(replacement); err != nil {
		return ServiceRecord{}, err
	}
	return replacement, nil
}

// SetServiceRuntimeIntent changes only Controller-owned operational state.
func SetServiceRuntimeIntent(
	record ServiceRecord,
	intent core.ServiceRuntimeIntent,
) (ServiceRecord, error) {
	if err := validateServiceRecord(record); err != nil {
		return ServiceRecord{}, err
	}
	replacement := record
	replacement.Runtime.RuntimeIntent = intent
	if err := validateServiceRecord(replacement); err != nil {
		return ServiceRecord{}, err
	}
	return replacement, nil
}

func serviceKey(id string) string { return servicePrefix + id }

func serviceNameKey(environmentID string, name string) string {
	return "/v1/indexes/services/by-name/environment/" + environmentID + "/" + encodeDynamicSegment(name)
}

func serviceOwnerPrefix(environmentID string) string {
	return "/v1/indexes/services/by-owner/environment/" + environmentID + "/"
}

func serviceOwnerKey(environmentID string, serviceID string) string {
	return serviceOwnerPrefix(environmentID) + serviceID
}

func validateServiceRecord(record ServiceRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindService, record.Desired.ID); err != nil {
		return err
	}
	if err := record.Desired.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if err := record.Runtime.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.Runtime.ServiceID != record.Desired.ID {
		return errs.New(errs.KindValidationFailed, "Service runtime identity does not match desired Service")
	}
	if record.Desired.Adapter == "" {
		if record.BackingNetworkID != "" {
			return errs.New(errs.KindValidationFailed, "ordinary Service cannot select a backing network")
		}
		return nil
	}
	if err := validateID(ids.KindNetwork, record.BackingNetworkID); err != nil {
		return errs.New(errs.KindValidationFailed, "adapter-backed Service requires a stable backing network id")
	}
	return nil
}

func encodeServiceRecord(record ServiceRecord) ([]byte, error) {
	if err := validateServiceRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("service", record)
}

func decodeServiceRecord(value []byte) (ServiceRecord, error) {
	record, err := decodeEnvelope[ServiceRecord](value, "service")
	if err != nil {
		return ServiceRecord{}, err
	}
	if err := validateServiceRecord(record); err != nil {
		return ServiceRecord{}, corruptRecord()
	}
	return record, nil
}
