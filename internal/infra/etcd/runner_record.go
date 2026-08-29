package etcd

import (
	"bytes"
	"net/netip"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumRunnerPersistenceBytes  = 64 << 10
	runnerContainerIDEncodedLength = 64
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

// BindRunnerContainerID records the rootful container identity once. The
// daemon-instance ownership evidence is intentionally stored elsewhere.
func BindRunnerContainerID(record RunnerRecord, taskID string, containerID string) (RunnerRecord, error) {
	if err := validateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	if record.ProvisioningState != RunnerProvisioningProvisioning || record.CreateTaskID != taskID {
		return RunnerRecord{}, errs.New(errs.KindStateConflict, "runner provisioning task does not own the record")
	}
	if !validLowerHex(containerID, runnerContainerIDEncodedLength) {
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
	record.ContainerID = ""
	if record.RuntimeEpoch == ^uint64(0) {
		return RunnerRecord{}, errs.New(errs.KindResourceInUse, "runner runtime epoch is exhausted")
	}
	record.RuntimeEpoch++
	return cloneRunnerRecord(record), nil
}

func TakeRunnerRuntimeCleanupOwnership(record RunnerRecord) (RunnerRecord, error) {
	if err := validateRunnerRecord(record); err != nil {
		return RunnerRecord{}, err
	}
	if record.RuntimeEpoch == ^uint64(0) {
		return RunnerRecord{}, errs.New(errs.KindResourceInUse, "runner runtime epoch is exhausted")
	}
	record.RuntimeEpoch++
	return cloneRunnerRecord(record), nil
}

func validateRunnerRecord(record RunnerRecord) error {
	if err := validateRunnerDesired(record.Desired); err != nil {
		return err
	}
	if record.RunnerID != record.Desired.ID {
		return errs.New(errs.KindValidationFailed, "runner desired and lifecycle identities do not match")
	}
	return validateRunnerLifecycle(record.RunnerLifecycleRecord)
}

func validateRunnerLifecycle(record RunnerLifecycleRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || record.RuntimeEpoch == 0 {
		return errs.New(errs.KindValidationFailed, "runner lifecycle identity is invalid")
	}
	if record.ProvisioningState != RunnerProvisioningProvisioning &&
		record.ProvisioningState != RunnerProvisioningReady &&
		record.ProvisioningState != RunnerProvisioningFailed {
		return errs.New(errs.KindValidationFailed, "runner provisioning state is invalid")
	}
	if ids.Validate(ids.KindTask, record.CreateTaskID) != nil || !validMarkerTime(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner lifecycle is invalid")
	}
	if record.ContainerID != "" && !validLowerHex(record.ContainerID, runnerContainerIDEncodedLength) {
		return errs.New(errs.KindValidationFailed, "runner container id is invalid")
	}
	if err := record.Allocation.Validate(); err != nil {
		return err
	}
	return nil
}

func validateRunnerDesired(desired RunnerDesiredRecord) error {
	if err := validateRunnerOwnership(desired); err != nil {
		return err
	}
	normalized, err := NormalizeRunnerDesired(desired)
	if err != nil || normalized.Slug != desired.Slug || normalized.GitHubURL != desired.GitHubURL ||
		normalized.ImageRef != desired.ImageRef || !slices.Equal(normalized.Labels, desired.Labels) {
		return errs.New(errs.KindValidationFailed, "runner desired state is not canonical")
	}
	return nil
}

func validateRunnerOwnership(desired RunnerDesiredRecord) error {
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
	return nil
}

func NormalizeRunnerDesired(desired RunnerDesiredRecord) (RunnerDesiredRecord, error) {
	if err := validateRunnerOwnership(desired); err != nil {
		return RunnerDesiredRecord{}, err
	}
	if err := validateRunnerSlug(desired.Slug); err != nil {
		return RunnerDesiredRecord{}, err
	}
	githubURL, err := CanonicalRunnerGitHubURL(desired.OwnerKind, desired.GitHubURL)
	if err != nil {
		return RunnerDesiredRecord{}, err
	}
	labels, err := canonicalRunnerLabels(desired.Labels)
	if err != nil {
		return RunnerDesiredRecord{}, err
	}
	if !validRunnerImageRef(desired.ImageRef) {
		return RunnerDesiredRecord{}, errs.New(errs.KindValidationFailed, "runner image_ref is invalid")
	}
	desired.GitHubURL = githubURL
	desired.Labels = labels
	return desired, nil
}

func validateRunnerSlug(value string) error {
	if !slug.Valid(value) {
		return errs.New(errs.KindValidationFailed, "runner slug must be a lowercase ASCII label of 1-63 bytes")
	}
	return nil
}

func RunnerName(runnerID string) (string, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return "", errs.New(errs.KindValidationFailed, "runner id is invalid")
	}
	return "gp-" + strings.ToLower(strings.TrimPrefix(runnerID, string(ids.KindRunner)+"_")), nil
}

func CanonicalRunnerGitHubURL(ownerKind RunnerOwnerKind, value string) (string, error) {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] >= 0x7f || value[index] == '\\' || value[index] == '%' {
			return "", errs.New(errs.KindValidationFailed, "runner github_url is invalid")
		}
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" {
		return "", errs.New(errs.KindValidationFailed, "runner github_url is invalid")
	}
	path := strings.TrimPrefix(parsed.Path, "/")
	if strings.HasSuffix(path, "//") {
		return "", errs.New(errs.KindValidationFailed, "runner github_url is invalid")
	}
	path = strings.TrimSuffix(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || !validGitHubOrganization(parts[0]) {
		return "", errs.New(errs.KindValidationFailed, "runner github_url organization is invalid")
	}
	switch ownerKind {
	case RunnerOwnerTenant:
		if len(parts) != 1 {
			return "", errs.New(errs.KindValidationFailed, "tenant runner github_url must identify an organization")
		}
	case RunnerOwnerProject:
		if len(parts) != 2 || !validGitHubRepository(parts[1]) {
			return "", errs.New(errs.KindValidationFailed, "project runner github_url must identify a repository")
		}
	default:
		return "", errs.New(errs.KindValidationFailed, "runner owner kind is invalid")
	}
	return "https://github.com/" + strings.ToLower(strings.Join(parts, "/")), nil
}

func validGitHubOrganization(value string) bool {
	if len(value) < 1 || len(value) > 39 || value[0] == '-' || value[len(value)-1] == '-' ||
		strings.Contains(value, "--") {
		return false
	}
	for index := range value {
		character := value[index]
		if !asciiAlphanumeric(character) && character != '-' {
			return false
		}
	}
	return true
}

func validGitHubRepository(value string) bool {
	if len(value) < 1 || len(value) > 100 || value == "." || value == ".." || strings.HasSuffix(value, ".") ||
		strings.HasSuffix(strings.ToLower(value), ".git") {
		return false
	}
	for index := range value {
		character := value[index]
		if !asciiAlphanumeric(character) && character != '.' && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func canonicalRunnerLabels(values []string) ([]string, error) {
	if len(values) > 32 {
		return nil, errs.New(errs.KindValidationFailed, "runner labels exceed the maximum of 32")
	}
	labels := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if len(value) < 1 || len(value) > 64 {
			return nil, errs.New(errs.KindValidationFailed, "runner labels are invalid")
		}
		for index := range value {
			character := value[index]
			if !asciiAlphanumeric(character) && character != '.' && character != '_' && character != '-' {
				return nil, errs.New(errs.KindValidationFailed, "runner labels are invalid")
			}
		}
		canonical := strings.ToLower(value)
		if canonical == "self-hosted" || canonical == "linux" || canonical == "x64" || canonical == "arm64" {
			return nil, errs.New(errs.KindValidationFailed, "runner label is reserved")
		}
		if _, exists := seen[canonical]; exists {
			return nil, errs.New(errs.KindValidationFailed, "runner labels must be case-insensitively unique")
		}
		seen[canonical] = struct{}{}
		labels = append(labels, canonical)
	}
	sort.Strings(labels)
	return labels, nil
}

func asciiAlphanumeric(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func validRunnerImageRef(value string) bool {
	return imageref.IsDigestPinned(value)
}

func validateRunnerObservation(record RunnerObservationRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || !validMarkerTime(record.ObservedAt) {
		return errs.New(errs.KindValidationFailed, "runner observation is invalid")
	}
	return nil
}

func encodeRunnerDesiredRecord(record RunnerDesiredRecord) ([]byte, error) {
	if err := validateRunnerDesired(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("runner_desired", record)
}

func decodeRunnerDesiredRecord(value []byte) (RunnerDesiredRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerDesiredRecord{}, errs.New(errs.KindInternal, "runner desired record is corrupt")
	}
	record, err := decodeEnvelope[RunnerDesiredRecord](value, "runner_desired")
	if err != nil || validateRunnerDesired(record) != nil {
		return RunnerDesiredRecord{}, errs.New(errs.KindInternal, "runner desired record is corrupt")
	}
	record.Labels = append([]string(nil), record.Labels...)
	return record, nil
}

func decodeRunnerDesiredAggregate(value []byte) (RunnerRecord, error) {
	desired, err := decodeRunnerDesiredRecord(value)
	if err != nil {
		return RunnerRecord{}, err
	}
	return RunnerRecord{Desired: desired}, nil
}

func encodeRunnerLifecycleRecord(record RunnerLifecycleRecord) ([]byte, error) {
	if err := validateRunnerLifecycle(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("runner_lifecycle", record)
}

func decodeRunnerLifecycleRecord(value []byte) (RunnerLifecycleRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerLifecycleRecord{}, errs.New(errs.KindInternal, "runner lifecycle record is corrupt")
	}
	record, err := decodeEnvelope[RunnerLifecycleRecord](value, "runner_lifecycle")
	if err != nil || validateRunnerLifecycle(record) != nil {
		return RunnerLifecycleRecord{}, errs.New(errs.KindInternal, "runner lifecycle record is corrupt")
	}
	return record, nil
}

func validateRunnerRuntimeOwnership(record RunnerRuntimeOwnershipRecord) error {
	expectedEndpoint := "unix:///run/groundplane/runners/" + record.RunnerID + "/xdg/docker.sock"
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil || record.RuntimeEpoch == 0 ||
		!utf8.ValidString(record.DaemonSocketEndpoint) || len(record.DaemonSocketEndpoint) > 240 ||
		record.DaemonSocketEndpoint != expectedEndpoint || record.SocketDevice == 0 || record.SocketInode == 0 ||
		!validMarkerTime(record.CreatedAt) || !validRunnerRuntimeNonce(record.DaemonInstanceNonce) {
		return errs.New(errs.KindValidationFailed, "runner runtime ownership is invalid")
	}
	return nil
}

func validRunnerRuntimeNonce(value string) bool {
	return validLowerHex(value, 64)
}

func validLowerHex(value string, length int) bool {
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

func encodeRunnerRuntimeOwnership(record RunnerRuntimeOwnershipRecord) ([]byte, error) {
	if err := validateRunnerRuntimeOwnership(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("runner_runtime_ownership", record)
}

func decodeRunnerRuntimeOwnership(value []byte) (RunnerRuntimeOwnershipRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerRuntimeOwnershipRecord{}, errs.New(errs.KindInternal, "runner runtime ownership record is corrupt")
	}
	record, err := decodeEnvelope[RunnerRuntimeOwnershipRecord](value, "runner_runtime_ownership")
	if err != nil || validateRunnerRuntimeOwnership(record) != nil {
		return RunnerRuntimeOwnershipRecord{}, errs.New(errs.KindInternal, "runner runtime ownership record is corrupt")
	}
	return record, nil
}

func sameRunnerRuntimeOwnershipBytes(left []byte, right []byte) bool {
	return bytes.Equal(left, right)
}

func encodeRunnerObservation(record RunnerObservationRecord) ([]byte, error) {
	if err := validateRunnerObservation(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("runner_observation", record)
}

func decodeRunnerObservation(value []byte) (RunnerObservationRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerObservationRecord{}, errs.New(errs.KindInternal, "runner observation is corrupt")
	}
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
	if err != nil || prefix.String() != value || prefix.Bits() != runnerallocation.RunnerSubnetBits {
		return netip.Prefix{}, errs.New(errs.KindInternal, "runner network allocation is corrupt")
	}
	return prefix, nil
}
