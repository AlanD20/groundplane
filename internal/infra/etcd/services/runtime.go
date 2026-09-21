package services

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
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

func ServiceRuntimeKey(serviceID string) string { return serviceRuntimePrefix + serviceID }

func NewServiceRuntimeRecord(record ServiceRecord) ServiceRuntimeRecord {
	return ServiceRuntimeRecord{
		EnvironmentID: record.EnvironmentID, ServiceID: record.Desired.ID,
		BackingNetworkID: record.BackingNetworkID, Runtime: record.Runtime,
	}
}

func validateServiceRuntimeRecord(record ServiceRuntimeRecord) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindService, record.ServiceID); err != nil {
		return err
	}
	if record.Runtime.ServiceID != record.ServiceID {
		return errs.New(errs.KindValidationFailed, "Service runtime identity does not match its sidecar")
	}
	if err := record.Runtime.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.BackingNetworkID != "" {
		if err := recordcodec.ValidateID(ids.KindNetwork, record.BackingNetworkID); err != nil {
			return err
		}
	}
	return nil
}

func EncodeServiceRuntimeRecord(record ServiceRuntimeRecord) ([]byte, error) {
	if err := validateServiceRuntimeRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("service_runtime", record)
}

// EncodeServiceRuntimeRecordStorage returns the exact value used by Blueprint
// final publication for a Service runtime source.
func EncodeServiceRuntimeRecordStorage(record ServiceRecord) ([]byte, error) {
	return EncodeServiceRuntimeRecord(NewServiceRuntimeRecord(record))
}

func DecodeServiceRuntimeRecord(value []byte) (ServiceRuntimeRecord, error) {
	record, err := recordcodec.Decode[ServiceRuntimeRecord](value, "service_runtime")
	if err != nil || validateServiceRuntimeRecord(record) != nil {
		return ServiceRuntimeRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func ServiceDesiredCondition(service etcdstore.Versioned[ServiceRecord]) etcdstore.Condition {
	return etcdstore.Condition{Key: service.Record.desiredFenceKey, ModRevision: service.Revision}
}

func ServiceRuntimeCondition(service etcdstore.Versioned[ServiceRecord]) etcdstore.Condition {
	return etcdstore.Condition{Key: ServiceRuntimeKey(service.Record.Desired.ID), ModRevision: service.Record.runtimeRevision}
}

func ServiceRuntimeRevision(service etcdstore.Versioned[ServiceRecord]) int64 {
	return service.Record.runtimeRevision
}
