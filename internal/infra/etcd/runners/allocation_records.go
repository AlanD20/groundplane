package runners

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	SystemPoolRegistryKey   = "/v1/indexes/system/by-network-pool/global/-"
	runnerHostSlotPrefix    = "/v1/runtime/runner-host-slots/"
	runnerTenantQuotaPrefix = "/v1/singletons/runner-tenant-quotas/"
)

type RunnerHostSlotRecord struct {
	Slot     uint32 `json:"slot"`
	RunnerID string `json:"runner_id"`
}

func RunnerHostSlotKey(slot uint32) string {
	return runnerHostSlotPrefix + RunnerHostSlotSegment(slot)
}

func RunnerHostSlotSegment(slot uint32) string {
	return fmt.Sprintf("%010d", slot)
}

func ParseRunnerHostSlotSegment(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || RunnerHostSlotSegment(uint32(parsed)) != value {
		return 0, errs.New(errs.KindValidationFailed, "runner host slot segment is invalid")
	}
	return uint32(parsed), nil
}

func validateRunnerHostSlotRecord(record RunnerHostSlotRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil {
		return errs.New(errs.KindValidationFailed, "runner host slot owner is invalid")
	}
	return nil
}

func EncodeRunnerHostSlotRecord(record RunnerHostSlotRecord) ([]byte, error) {
	if err := validateRunnerHostSlotRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_host_slot", record)
}

func DecodeRunnerHostSlotRecord(value []byte) (RunnerHostSlotRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return RunnerHostSlotRecord{}, CorruptRunnerHostSlotRecord()
	}
	record, err := recordcodec.Decode[RunnerHostSlotRecord](value, "runner_host_slot")
	if err != nil || validateRunnerHostSlotRecord(record) != nil {
		return RunnerHostSlotRecord{}, CorruptRunnerHostSlotRecord()
	}
	return record, nil
}

func DecodeRunnerTenantQuota(value []byte) (runnerallocation.RunnerTenantQuota, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return runnerallocation.RunnerTenantQuota{}, CorruptRunnerTenantQuota()
	}
	quota, err := recordcodec.Decode[runnerallocation.RunnerTenantQuota](value, "runner_tenant_quota")
	if err != nil || quota.Validate() != nil {
		return runnerallocation.RunnerTenantQuota{}, CorruptRunnerTenantQuota()
	}
	return quota, nil
}

func CorruptRunnerHostSlotRecord() error {
	return errs.New(errs.KindInternal, "runner host slot record is corrupt")
}

func DecodeSystemPoolRegistry(value []byte) (runnerallocation.SystemPoolRegistry, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return runnerallocation.SystemPoolRegistry{}, CorruptSystemPoolRegistry()
	}
	registry, err := recordcodec.Decode[runnerallocation.SystemPoolRegistry](value, "system_pool_registry")
	if err != nil || registry.ValidateReservations() != nil {
		return runnerallocation.SystemPoolRegistry{}, CorruptSystemPoolRegistry()
	}
	return registry, nil
}

func CorruptSystemPoolRegistry() error {
	return errs.New(errs.KindInternal, "system pool registry is corrupt")
}

func CorruptRunnerTenantQuota() error {
	return errs.New(errs.KindInternal, "runner tenant quota is corrupt")
}

func RunnerTenantQuotaKey(tenantID string) string { return runnerTenantQuotaPrefix + tenantID }
