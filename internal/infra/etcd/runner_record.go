package etcd

import (
	"net/netip"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerOwnerKind string

const (
	RunnerOwnerTenant  RunnerOwnerKind = "tenant"
	RunnerOwnerProject RunnerOwnerKind = "project"
)

type RunnerProvisioningState string

const (
	RunnerProvisioningProvisioning RunnerProvisioningState = "provisioning"
	RunnerProvisioningReady        RunnerProvisioningState = "ready"
	RunnerProvisioningFailed       RunnerProvisioningState = "failed"
)

// RunnerDesiredRecord is the infra-owned durable input for one Runner. It is
// deliberately not a core or public API model.
type RunnerDesiredRecord struct {
	ID        string          `json:"id"`
	OwnerKind RunnerOwnerKind `json:"owner_kind"`
	OwnerID   string          `json:"owner_id"`
	TenantID  string          `json:"tenant_id"`
	Labels    []string        `json:"labels,omitempty"`
}

// RunnerHostAllocationRecord is the exact scarce allocation retained across
// create retry and removal finalization. Registration credentials never enter
// this record.
type RunnerHostAllocationRecord struct {
	Slot        uint32 `json:"slot"`
	HostUID     uint32 `json:"host_uid"`
	SubUIDStart uint32 `json:"subuid_start"`
	SubUIDCount uint32 `json:"subuid_count"`
	SubGIDStart uint32 `json:"subgid_start"`
	SubGIDCount uint32 `json:"subgid_count"`
	NetworkCIDR string `json:"network_cidr"`
}

type RunnerRecord struct {
	Desired           RunnerDesiredRecord        `json:"desired"`
	ProvisioningState RunnerProvisioningState    `json:"provisioning_state"`
	CreateTaskID      string                     `json:"create_task_id"`
	Allocation        RunnerHostAllocationRecord `json:"allocation"`
	CreatedAt         time.Time                  `json:"created_at"`
}

// RunnerObservationRecord is replaceable observed runtime state. It is not a
// desired-state field and is never used as a lifecycle fence.
type RunnerObservationRecord struct {
	RunnerID   string    `json:"runner_id"`
	Online     bool      `json:"online"`
	ObservedAt time.Time `json:"observed_at"`
}

func NewProvisioningRunner(
	desired RunnerDesiredRecord,
	allocation RunnerHostAllocationRecord,
	taskID string,
	createdAt time.Time,
) (RunnerRecord, error) {
	record := RunnerRecord{
		Desired: desired, ProvisioningState: RunnerProvisioningProvisioning,
		CreateTaskID: taskID, Allocation: allocation, CreatedAt: createdAt,
	}
	if err := validateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	return cloneRunnerRecord(record), nil
}

func CompleteRunnerProvisioning(record RunnerRecord, taskID string, succeeded bool) (RunnerRecord, error) {
	if err := validateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	if record.ProvisioningState != RunnerProvisioningProvisioning || record.CreateTaskID != taskID {
		return RunnerRecord{}, errs.New(errs.KindStateConflict, "runner provisioning task does not own the record")
	}
	if succeeded {
		record.ProvisioningState = RunnerProvisioningReady
	} else {
		record.ProvisioningState = RunnerProvisioningFailed
	}
	return cloneRunnerRecord(record), nil
}

func RetryRunnerProvisioning(record RunnerRecord, sourceTaskID string, retryTaskID string) (RunnerRecord, error) {
	if err := validateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	if record.ProvisioningState != RunnerProvisioningFailed || record.CreateTaskID != sourceTaskID {
		return RunnerRecord{}, errs.New(errs.KindStateConflict, "runner is not available for provisioning retry")
	}
	if ids.Validate(ids.KindTask, retryTaskID) != nil {
		return RunnerRecord{}, errs.New(errs.KindValidationFailed, "retry task id is invalid")
	}
	record.ProvisioningState = RunnerProvisioningProvisioning
	record.CreateTaskID = retryTaskID
	return cloneRunnerRecord(record), nil
}

func validateRunnerRecord(record RunnerRecord) error {
	if err := validateRunnerDesired(record.Desired); err != nil {
		return err
	}
	if record.ProvisioningState != RunnerProvisioningProvisioning &&
		record.ProvisioningState != RunnerProvisioningReady &&
		record.ProvisioningState != RunnerProvisioningFailed {
		return errs.New(errs.KindValidationFailed, "runner provisioning state is invalid")
	}
	if ids.Validate(ids.KindTask, record.CreateTaskID) != nil ||
		record.CreatedAt.IsZero() || !record.CreatedAt.Equal(record.CreatedAt.UTC()) {
		return errs.New(errs.KindValidationFailed, "runner lifecycle is invalid")
	}
	if err := validateRunnerAllocation(record.Allocation); err != nil {
		return err
	}
	return nil
}

func validateRunnerDesired(desired RunnerDesiredRecord) error {
	if ids.Validate(ids.KindRunner, desired.ID) != nil || ids.Validate(ids.KindTenant, desired.TenantID) != nil {
		return errs.New(errs.KindValidationFailed, "runner identity is invalid")
	}
	switch desired.OwnerKind {
	case RunnerOwnerTenant:
		if desired.OwnerID != desired.TenantID {
			return errs.New(errs.KindValidationFailed, "tenant-owned runner has mismatched ownership")
		}
	case RunnerOwnerProject:
		if ids.Validate(ids.KindProject, desired.OwnerID) != nil {
			return errs.New(errs.KindValidationFailed, "project-owned runner owner is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "runner owner kind is invalid")
	}
	for _, label := range desired.Labels {
		if label == "" || !utf8.ValidString(label) {
			return errs.New(errs.KindValidationFailed, "runner labels are invalid")
		}
	}
	return nil
}

func validateRunnerAllocation(allocation RunnerHostAllocationRecord) error {
	prefix, err := ipam.ParseIPv4Prefix(allocation.NetworkCIDR)
	if err != nil || prefix.String() != allocation.NetworkCIDR || prefix.Bits() != runnerSubnetBits ||
		allocation.SubUIDCount != runnerSubordinateBlockSize ||
		allocation.SubGIDCount != runnerSubordinateBlockSize {
		return errs.New(errs.KindValidationFailed, "runner allocation is invalid")
	}
	return nil
}

func validateRunnerObservation(record RunnerObservationRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || record.ObservedAt.IsZero() ||
		!record.ObservedAt.Equal(record.ObservedAt.UTC()) {
		return errs.New(errs.KindValidationFailed, "runner observation is invalid")
	}
	return nil
}

func encodeRunnerRecord(record RunnerRecord) ([]byte, error) {
	if err := validateRunnerRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("runner", record)
}

func decodeRunnerRecord(value []byte) (RunnerRecord, error) {
	record, err := decodeEnvelope[RunnerRecord](value, "runner")
	if err != nil || validateRunnerRecord(record) != nil {
		return RunnerRecord{}, errs.New(errs.KindInternal, "runner record is corrupt")
	}
	return cloneRunnerRecord(record), nil
}

func encodeRunnerObservation(record RunnerObservationRecord) ([]byte, error) {
	if err := validateRunnerObservation(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("runner_observation", record)
}

func decodeRunnerObservation(value []byte) (RunnerObservationRecord, error) {
	record, err := decodeEnvelope[RunnerObservationRecord](value, "runner_observation")
	if err != nil || validateRunnerObservation(record) != nil {
		return RunnerObservationRecord{}, errs.New(errs.KindInternal, "runner observation is corrupt")
	}
	return record, nil
}

func cloneRunnerRecord(record RunnerRecord) RunnerRecord {
	record.Desired.Labels = append([]string(nil), record.Desired.Labels...)
	return record
}

func runnerAllocationPrefix(value string) (netip.Prefix, error) {
	prefix, err := ipam.ParseIPv4Prefix(value)
	if err != nil || prefix.String() != value || prefix.Bits() != runnerSubnetBits {
		return netip.Prefix{}, errs.New(errs.KindInternal, "runner network allocation is corrupt")
	}
	return prefix, nil
}
