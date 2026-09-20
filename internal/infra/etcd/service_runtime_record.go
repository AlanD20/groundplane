package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const serviceRuntimePrefix = "/v1/records/service-runtimes/"

// ServiceRuntimeRecord is the sole mutable Service state. Desired Service
// fields are deliberately absent and can only come from a selected revision.
type ServiceRuntimeRecord struct {
	EnvironmentID    string              `json:"environment_id"`
	ServiceID        string              `json:"service_id"`
	BackingNetworkID string              `json:"backing_network_id,omitempty"`
	Runtime          core.ServiceRuntime `json:"runtime"`
}

func serviceRuntimeKey(serviceID string) string { return serviceRuntimePrefix + serviceID }

func newServiceRuntimeRecord(record ServiceRecord) ServiceRuntimeRecord {
	return ServiceRuntimeRecord{
		EnvironmentID: record.EnvironmentID, ServiceID: record.Desired.ID,
		BackingNetworkID: record.BackingNetworkID, Runtime: record.Runtime,
	}
}

func validateServiceRuntimeRecord(record ServiceRuntimeRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindService, record.ServiceID); err != nil {
		return err
	}
	if record.Runtime.ServiceID != record.ServiceID {
		return errs.New(errs.KindValidationFailed, "Service runtime identity does not match its sidecar")
	}
	if err := record.Runtime.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.BackingNetworkID != "" {
		if err := validateID(ids.KindNetwork, record.BackingNetworkID); err != nil {
			return err
		}
	}
	return nil
}

func encodeServiceRuntimeRecord(record ServiceRuntimeRecord) ([]byte, error) {
	if err := validateServiceRuntimeRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("service_runtime", record)
}

// EncodeServiceRuntimeRecordStorage returns the exact value used by Blueprint
// final publication for a Service runtime source.
func EncodeServiceRuntimeRecordStorage(record ServiceRecord) ([]byte, error) {
	return encodeServiceRuntimeRecord(newServiceRuntimeRecord(record))
}

func decodeServiceRuntimeRecord(value []byte) (ServiceRuntimeRecord, error) {
	record, err := decodeEnvelope[ServiceRuntimeRecord](value, "service_runtime")
	if err != nil || validateServiceRuntimeRecord(record) != nil {
		return ServiceRuntimeRecord{}, corruptRecord()
	}
	return record, nil
}

func serviceDesiredCondition(service Versioned[ServiceRecord]) etcdstore.Condition {
	return etcdstore.Condition{Key: service.Record.desiredFenceKey, ModRevision: service.Revision}
}

func serviceRuntimeCondition(service Versioned[ServiceRecord]) etcdstore.Condition {
	return etcdstore.Condition{Key: serviceRuntimeKey(service.Record.Desired.ID), ModRevision: service.Record.runtimeRevision}
}

func ServiceRuntimeRevision(service Versioned[ServiceRecord]) int64 {
	return service.Record.runtimeRevision
}
