package runners

import (
	"bytes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/netip"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumRunnerPersistenceBytes  = 64 << 10
	RunnerContainerIDEncodedLength = 64
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
	Slug      string          `json:"slug"`
	OwnerKind RunnerOwnerKind `json:"owner_kind"`
	OwnerID   string          `json:"owner_id"`
	TenantID  string          `json:"tenant_id"`
	GitHubURL string          `json:"github_url"`
	Labels    []string        `json:"labels,omitempty"`
	ImageRef  string          `json:"image_ref"`
}

type RunnerRecord struct {
	Desired RunnerDesiredRecord
	RunnerLifecycleRecord
	LifecycleRevision int64
}

type RunnerLifecycleRecord struct {
	RunnerID          string                                      `json:"runner_id"`
	ProvisioningState RunnerProvisioningState                     `json:"provisioning_state"`
	CreateTaskID      string                                      `json:"create_task_id"`
	Allocation        runnerallocation.RunnerHostAllocationRecord `json:"allocation"`
	CreatedAt         time.Time                                   `json:"created_at"`
	ContainerID       string                                      `json:"container_id,omitempty"`
	RuntimeEpoch      uint64                                      `json:"runtime_epoch"`
}

type RunnerRuntimeOwnershipRecord struct {
	RunnerID             string    `json:"runner_id"`
	RuntimeEpoch         uint64    `json:"runtime_epoch"`
	DaemonSocketEndpoint string    `json:"daemon_socket_endpoint"`
	DaemonInstanceNonce  string    `json:"daemon_instance_nonce"`
	SocketDevice         uint64    `json:"socket_device"`
	SocketInode          uint64    `json:"socket_inode"`
	CreatedAt            time.Time `json:"created_at"`
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
	allocation runnerallocation.RunnerHostAllocationRecord,
	taskID string,
	createdAt time.Time,
) (RunnerRecord, error) {
	normalized, err := NormalizeRunnerDesired(desired)
	if err != nil {
		return RunnerRecord{}, err
	}
	record := RunnerRecord{
		Desired: normalized,
		RunnerLifecycleRecord: RunnerLifecycleRecord{
			RunnerID: normalized.ID, ProvisioningState: RunnerProvisioningProvisioning,
			CreateTaskID: taskID, Allocation: allocation, CreatedAt: createdAt, RuntimeEpoch: 1,
		},
	}
	if err := ValidateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	return CloneRunnerRecord(record), nil
}

func CompleteRunnerProvisioning(record RunnerRecord, taskID string, succeeded bool) (RunnerRecord, error) {
	if err := ValidateRunnerRecord(record); err != nil {
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
	return CloneRunnerRecord(record), nil
}

// BindRunnerContainerID records the rootful container identity once. The
// daemon-instance ownership evidence is intentionally stored elsewhere.
func BindRunnerContainerID(record RunnerRecord, taskID string, containerID string) (RunnerRecord, error) {
	if err := ValidateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	if record.ProvisioningState != RunnerProvisioningProvisioning || record.CreateTaskID != taskID {
		return RunnerRecord{}, errs.New(errs.KindStateConflict, "runner provisioning task does not own the record")
	}
	if !ValidLowerHex(containerID, RunnerContainerIDEncodedLength) {
		return RunnerRecord{}, errs.New(errs.KindValidationFailed, "runner container id is invalid")
	}
	if record.ContainerID != "" && record.ContainerID != containerID {
		return RunnerRecord{}, errs.New(errs.KindStateConflict, "runner container id is immutable")
	}
	if record.ContainerID == "" {
		if record.RuntimeEpoch == ^uint64(0) {
			return RunnerRecord{}, errs.New(errs.KindResourceInUse, "runner runtime epoch is exhausted")
		}
		record.RuntimeEpoch++
	}
	record.ContainerID = containerID
	return CloneRunnerRecord(record), nil
}

func RetryRunnerProvisioning(record RunnerRecord, sourceTaskID string, retryTaskID string) (RunnerRecord, error) {
	if err := ValidateRunnerRecord(record); err != nil {
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
	record.ContainerID = ""
	if record.RuntimeEpoch == ^uint64(0) {
		return RunnerRecord{}, errs.New(errs.KindResourceInUse, "runner runtime epoch is exhausted")
	}
	record.RuntimeEpoch++
	return CloneRunnerRecord(record), nil
}

func TakeRunnerRuntimeCleanupOwnership(record RunnerRecord) (RunnerRecord, error) {
	if err := ValidateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	if record.RuntimeEpoch == ^uint64(0) {
		return RunnerRecord{}, errs.New(errs.KindResourceInUse, "runner runtime epoch is exhausted")
	}
	record.RuntimeEpoch++
	return CloneRunnerRecord(record), nil
}

func ValidateRunnerRecord(record RunnerRecord) error {
	if err := ValidateRunnerDesired(record.Desired); err != nil {
		return err
	}
	if record.RunnerID != record.Desired.ID {
		return errs.New(errs.KindValidationFailed, "runner desired and lifecycle identities do not match")
	}
	return ValidateRunnerLifecycle(record.RunnerLifecycleRecord)
}

func ValidateRunnerLifecycle(record RunnerLifecycleRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || record.RuntimeEpoch == 0 {
		return errs.New(errs.KindValidationFailed, "runner lifecycle identity is invalid")
	}
	if record.ProvisioningState != RunnerProvisioningProvisioning &&
		record.ProvisioningState != RunnerProvisioningReady &&
		record.ProvisioningState != RunnerProvisioningFailed {
		return errs.New(errs.KindValidationFailed, "runner provisioning state is invalid")
	}
	if ids.Validate(ids.KindTask, record.CreateTaskID) != nil || !recordcodec.IsCanonicalUTC(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner lifecycle is invalid")
	}
	if record.ContainerID != "" && !ValidLowerHex(record.ContainerID, RunnerContainerIDEncodedLength) {
		return errs.New(errs.KindValidationFailed, "runner container id is invalid")
	}
	if err := record.Allocation.Validate(); err != nil {
		return err
	}
	return nil
}

func ValidateRunnerObservation(record RunnerObservationRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || !recordcodec.IsCanonicalUTC(record.ObservedAt) {
		return errs.New(errs.KindValidationFailed, "runner observation is invalid")
	}
	return nil
}

func ValidateRunnerRuntimeOwnership(record RunnerRuntimeOwnershipRecord) error {
	expectedEndpoint := "unix:///run/groundplane/runners/" + record.RunnerID + "/xdg/docker.sock"
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || record.RuntimeEpoch == 0 ||
		!utf8.ValidString(record.DaemonSocketEndpoint) || len(record.DaemonSocketEndpoint) > 240 ||
		record.DaemonSocketEndpoint != expectedEndpoint || record.SocketDevice == 0 || record.SocketInode == 0 ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) || !validRunnerRuntimeNonce(record.DaemonInstanceNonce) {
		return errs.New(errs.KindValidationFailed, "runner runtime ownership is invalid")
	}
	return nil
}

func validRunnerRuntimeNonce(value string) bool {
	return ValidLowerHex(value, 64)
}

func ValidLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func EncodeRunnerRuntimeOwnership(record RunnerRuntimeOwnershipRecord) ([]byte, error) {
	if err := ValidateRunnerRuntimeOwnership(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_runtime_ownership", record)
}

func DecodeRunnerRuntimeOwnership(value []byte) (RunnerRuntimeOwnershipRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return RunnerRuntimeOwnershipRecord{}, errs.New(errs.KindInternal, "runner runtime ownership record is corrupt")
	}
	record, err := recordcodec.Decode[RunnerRuntimeOwnershipRecord](value, "runner_runtime_ownership")
	if err != nil || ValidateRunnerRuntimeOwnership(record) != nil {
		return RunnerRuntimeOwnershipRecord{}, errs.New(errs.KindInternal, "runner runtime ownership record is corrupt")
	}
	return record, nil
}

func SameRunnerRuntimeOwnershipBytes(left []byte, right []byte) bool {
	return bytes.Equal(left, right)
}

func EncodeRunnerObservation(record RunnerObservationRecord) ([]byte, error) {
	if err := ValidateRunnerObservation(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_observation", record)
}

func DecodeRunnerObservation(value []byte) (RunnerObservationRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return RunnerObservationRecord{}, errs.New(errs.KindInternal, "runner observation is corrupt")
	}
	record, err := recordcodec.Decode[RunnerObservationRecord](value, "runner_observation")
	if err != nil || ValidateRunnerObservation(record) != nil {
		return RunnerObservationRecord{}, errs.New(errs.KindInternal, "runner observation is corrupt")
	}
	return record, nil
}

func CloneRunnerRecord(record RunnerRecord) RunnerRecord {
	record.Desired.Labels = append([]string(nil), record.Desired.Labels...)
	return record
}

func RunnerAllocationPrefix(value string) (netip.Prefix, error) {
	prefix, err := ipam.ParseIPv4Prefix(value)
	if err != nil || prefix.String() != value || prefix.Bits() != runnerallocation.RunnerSubnetBits {
		return netip.Prefix{}, errs.New(errs.KindInternal, "runner network allocation is corrupt")
	}
	return prefix, nil
}
