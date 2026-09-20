package etcd

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	systemPoolRegistryKey   = "/v1/indexes/system/by-network-pool/global/-"
	runnerHostSlotPrefix    = "/v1/runtime/runner-host-slots/"
	runnerTenantQuotaPrefix = "/v1/singletons/runner-tenant-quotas/"
)

type RunnerHostSlotRecord struct {
	Slot     uint32 `json:"slot"`
	RunnerID string `json:"runner_id"`
}

func runnerHostSlotKey(slot uint32) string {
	return runnerHostSlotPrefix + runnerHostSlotSegment(slot)
}

func runnerHostSlotSegment(slot uint32) string {
	return fmt.Sprintf("%010d", slot)
}

func parseRunnerHostSlotSegment(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || runnerHostSlotSegment(uint32(parsed)) != value {
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

func encodeRunnerHostSlotRecord(record RunnerHostSlotRecord) ([]byte, error) {
	if err := validateRunnerHostSlotRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_host_slot", record)
}

func decodeRunnerHostSlotRecord(value []byte) (RunnerHostSlotRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerHostSlotRecord{}, corruptRunnerHostSlotRecord()
	}
	record, err := recordcodec.Decode[RunnerHostSlotRecord](value, "runner_host_slot")
	if err != nil || validateRunnerHostSlotRecord(record) != nil {
		return RunnerHostSlotRecord{}, corruptRunnerHostSlotRecord()
	}
	return record, nil
}

func decodeRunnerTenantQuota(value []byte) (runnerallocation.RunnerTenantQuota, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return runnerallocation.RunnerTenantQuota{}, corruptRunnerTenantQuota()
	}
	quota, err := recordcodec.Decode[runnerallocation.RunnerTenantQuota](value, "runner_tenant_quota")
	if err != nil || quota.Validate() != nil {
		return runnerallocation.RunnerTenantQuota{}, corruptRunnerTenantQuota()
	}
	return quota, nil
}

func corruptRunnerHostSlotRecord() error {
	return errs.New(errs.KindInternal, "runner host slot record is corrupt")
}

func decodeSystemPoolRegistry(value []byte) (runnerallocation.SystemPoolRegistry, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return runnerallocation.SystemPoolRegistry{}, corruptSystemPoolRegistry()
	}
	registry, err := recordcodec.Decode[runnerallocation.SystemPoolRegistry](value, "system_pool_registry")
	if err != nil || registry.ValidateReservations() != nil {
		return runnerallocation.SystemPoolRegistry{}, corruptSystemPoolRegistry()
	}
	return registry, nil
}

func corruptSystemPoolRegistry() error {
	return errs.New(errs.KindInternal, "system pool registry is corrupt")
}

func corruptRunnerTenantQuota() error {
	return errs.New(errs.KindInternal, "runner tenant quota is corrupt")
}

func runnerTenantQuotaKey(tenantID string) string { return runnerTenantQuotaPrefix + tenantID }
