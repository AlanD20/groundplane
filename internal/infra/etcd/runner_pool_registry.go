package etcd

import (
	"fmt"
	"math"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	commonconfig "github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	runnerSubnetBits           = 29
	maximumTenantRunners       = 5
	runnerSubordinateBlockSize = uint32(65536)
	systemPoolRegistryKey      = "/v1/indexes/system/by-network-pool/global/-"
	runnerHostSlotPrefix       = "/v1/runtime/runner-host-slots/"
	runnerTenantQuotaPrefix    = "/v1/singletons/runner-tenant-quotas/"
)

type RunnerHostPoolConfig struct {
	HostUIDStart uint32
	HostUIDEnd   uint32
	SubUIDStart  uint32
	SubUIDEnd    uint32
	SubGIDStart  uint32
	SubGIDEnd    uint32
}

type RunnerAllocationConfig struct {
	SystemPool netip.Prefix
	RunnerPool netip.Prefix
	HostPool   RunnerHostPoolConfig
}

func RunnerAllocationConfigFromPools(pools commonconfig.AllocationPools) RunnerAllocationConfig {
	return RunnerAllocationConfig{
		SystemPool: pools.System,
		RunnerPool: pools.Runner.Network,
		HostPool: RunnerHostPoolConfig{
			HostUIDStart: pools.Runner.HostUID.First, HostUIDEnd: pools.Runner.HostUID.Last,
			SubUIDStart: pools.Runner.SubUID.First, SubUIDEnd: pools.Runner.SubUID.Last,
			SubGIDStart: pools.Runner.SubGID.First, SubGIDEnd: pools.Runner.SubGID.Last,
		},
	}
}

func (config RunnerHostPoolConfig) SlotCount() (uint32, error) {
	if config.HostUIDEnd < config.HostUIDStart || config.SubUIDEnd < config.SubUIDStart ||
		config.SubGIDEnd < config.SubGIDStart {
		return 0, errs.New(errs.KindValidationFailed, "runner host allocation ranges are invalid")
	}
	hostSlots := uint64(config.HostUIDEnd) - uint64(config.HostUIDStart) + 1
	subUIDValues := uint64(config.SubUIDEnd) - uint64(config.SubUIDStart) + 1
	subGIDValues := uint64(config.SubGIDEnd) - uint64(config.SubGIDStart) + 1
	if hostSlots < maximumTenantRunners || hostSlots > math.MaxUint32 ||
		subUIDValues != hostSlots*uint64(runnerSubordinateBlockSize) ||
		subGIDValues != hostSlots*uint64(runnerSubordinateBlockSize) {
		return 0, errs.New(
			errs.KindValidationFailed,
			"runner host allocation ranges must define equal fixed-size slots",
		)
	}
	return uint32(hostSlots), nil
}

func (config RunnerAllocationConfig) Validate() (uint32, error) {
	if !config.SystemPool.IsValid() || !config.SystemPool.Addr().Is4() || config.SystemPool != config.SystemPool.Masked() ||
		!config.RunnerPool.IsValid() || !config.RunnerPool.Addr().Is4() || config.RunnerPool != config.RunnerPool.Masked() ||
		config.RunnerPool.Bits() <= config.SystemPool.Bits() || config.RunnerPool.Bits() > runnerSubnetBits ||
		!config.SystemPool.Contains(config.RunnerPool.Addr()) {
		return 0, errs.New(errs.KindValidationFailed, "runner network pool must be a canonical IPv4 child of the system pool")
	}
	slots, err := config.HostPool.SlotCount()
	if err != nil {
		return 0, err
	}
	networkSlots := uint64(1) << uint(runnerSubnetBits-config.RunnerPool.Bits())
	if networkSlots < uint64(slots) {
		return 0, errs.New(errs.KindValidationFailed, "runner network pool is smaller than the host allocation pool")
	}
	return slots, nil
}

func (config RunnerHostPoolConfig) Allocation(slot uint32, network netip.Prefix) (RunnerHostAllocationRecord, error) {
	slots, err := config.SlotCount()
	if err != nil {
		return RunnerHostAllocationRecord{}, err
	}
	if slot >= slots {
		return RunnerHostAllocationRecord{}, errs.New(
			errs.KindResourceInUse,
			"runner host allocation pool is exhausted",
		)
	}
	allocation := RunnerHostAllocationRecord{
		Slot:        slot,
		HostUID:     config.HostUIDStart + slot,
		SubUIDStart: uint32(uint64(config.SubUIDStart) + uint64(slot)*uint64(runnerSubordinateBlockSize)),
		SubUIDCount: runnerSubordinateBlockSize,
		SubGIDStart: uint32(uint64(config.SubGIDStart) + uint64(slot)*uint64(runnerSubordinateBlockSize)),
		SubGIDCount: runnerSubordinateBlockSize,
		NetworkCIDR: network.String(),
	}
	if err := validateRunnerAllocation(allocation); err != nil {
		return RunnerHostAllocationRecord{}, err
	}
	return allocation, nil
}

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
	return encodeEnvelope("runner_host_slot", record)
}

func decodeRunnerHostSlotRecord(value []byte) (RunnerHostSlotRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerHostSlotRecord{}, corruptRunnerHostSlotRecord()
	}
	record, err := decodeEnvelope[RunnerHostSlotRecord](value, "runner_host_slot")
	if err != nil || validateRunnerHostSlotRecord(record) != nil {
		return RunnerHostSlotRecord{}, corruptRunnerHostSlotRecord()
	}
	return record, nil
}

func decodeRunnerTenantQuota(value []byte) (RunnerTenantQuota, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerTenantQuota{}, corruptRunnerTenantQuota()
	}
	quota, err := decodeEnvelope[RunnerTenantQuota](value, "runner_tenant_quota")
	if err != nil || validateRunnerTenantQuota(quota) != nil {
		return RunnerTenantQuota{}, corruptRunnerTenantQuota()
	}
	return quota, nil
}

func corruptRunnerHostSlotRecord() error {
	return errs.New(errs.KindInternal, "runner host slot record is corrupt")
}

type SystemPoolRegistry struct {
	RunnerNetworkPool string            `json:"runner_network_pool"`
	Reservations      map[string]string `json:"reservations"`
}

func (registry SystemPoolRegistry) ReserveRunner(
	root netip.Prefix,
	runnerID string,
) (SystemPoolRegistry, netip.Prefix, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, errs.New(errs.KindValidationFailed, "runner id is invalid")
	}
	owner := runnerPoolOwner(runnerID)
	if registry.RunnerNetworkPool != root.String() {
		return SystemPoolRegistry{}, netip.Prefix{}, errs.New(errs.KindStateConflict, "runner network pool is not reserved")
	}
	reserved, err := registry.runnerPrefixes(root)
	if err != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, err
	}
	if value, exists := registry.Reservations[owner]; exists {
		prefix, parseErr := ipam.ParseIPv4Prefix(value)
		if parseErr != nil {
			return SystemPoolRegistry{}, netip.Prefix{}, corruptSystemPoolRegistry()
		}
		next := SystemPoolRegistry{
			RunnerNetworkPool: registry.RunnerNetworkPool,
			Reservations:      make(map[string]string, len(registry.Reservations)),
		}
		for key, reservation := range registry.Reservations {
			next.Reservations[key] = reservation
		}
		return next, prefix, nil
	}
	candidate, err := ipam.FirstAvailableChild(root, runnerSubnetBits, reserved)
	if err != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, errs.New(
			errs.KindResourceInUse,
			"runner network pool is exhausted",
		)
	}
	next := SystemPoolRegistry{
		RunnerNetworkPool: registry.RunnerNetworkPool,
		Reservations:      make(map[string]string, len(registry.Reservations)+1),
	}
	for key, value := range registry.Reservations {
		next.Reservations[key] = value
	}
	next.Reservations[owner] = candidate.String()
	return next, candidate, nil
}

func (registry SystemPoolRegistry) ReleaseRunner(runnerID string, subnet string) (SystemPoolRegistry, error) {
	if err := validateRunnerPoolReservation(registry); err != nil {
		return SystemPoolRegistry{}, err
	}
	owner := runnerPoolOwner(runnerID)
	if registry.Reservations[owner] != subnet {
		return SystemPoolRegistry{}, errs.New(
			errs.KindStateConflict,
			"runner network allocation does not match its owner",
		)
	}
	next := SystemPoolRegistry{
		RunnerNetworkPool: registry.RunnerNetworkPool,
		Reservations:      make(map[string]string, len(registry.Reservations)-1),
	}
	for key, value := range registry.Reservations {
		if key != owner {
			next.Reservations[key] = value
		}
	}
	return next, nil
}

func decodeSystemPoolRegistry(value []byte) (SystemPoolRegistry, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return SystemPoolRegistry{}, corruptSystemPoolRegistry()
	}
	registry, err := decodeEnvelope[SystemPoolRegistry](value, "system_pool_registry")
	if err != nil || validateRunnerPoolReservation(registry) != nil {
		return SystemPoolRegistry{}, corruptSystemPoolRegistry()
	}
	return registry, nil
}

func validateRunnerPoolReservation(registry SystemPoolRegistry) error {
	runnerPool, err := ipam.ParseIPv4Prefix(registry.RunnerNetworkPool)
	if err != nil || runnerPool.String() != registry.RunnerNetworkPool || runnerPool.Bits() > runnerSubnetBits ||
		registry.Reservations == nil {
		return corruptSystemPoolRegistry()
	}
	for owner, value := range registry.Reservations {
		prefix, parseErr := ipam.ParseIPv4Prefix(value)
		if strings.TrimSpace(owner) == "" || parseErr != nil || prefix.String() != value {
			return corruptSystemPoolRegistry()
		}
		if strings.HasPrefix(owner, "runner/") {
			if ids.Validate(ids.KindRunner, strings.TrimPrefix(owner, "runner/")) != nil ||
				prefix.Bits() != runnerSubnetBits || !runnerPool.Contains(prefix.Addr()) {
				return corruptSystemPoolRegistry()
			}
			continue
		}
		if prefix.Overlaps(runnerPool) {
			return corruptSystemPoolRegistry()
		}
	}
	return nil
}

func (registry SystemPoolRegistry) runnerPrefixes(root netip.Prefix) ([]netip.Prefix, error) {
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() {
		return nil, errs.New(errs.KindValidationFailed, "runner pool must be a canonical IPv4 pool")
	}
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	for owner, value := range registry.Reservations {
		if !strings.HasPrefix(owner, "runner/") {
			continue
		}
		prefix, err := ipam.ParseIPv4Prefix(value)
		if err != nil || ids.Validate(ids.KindRunner, strings.TrimPrefix(owner, "runner/")) != nil ||
			prefix.String() != value || prefix.Bits() != runnerSubnetBits ||
			ipam.ValidateChild(root, prefix, reserved) != nil {
			return nil, corruptSystemPoolRegistry()
		}
		reserved = append(reserved, prefix)
	}
	return reserved, nil
}

func validateSystemPoolRegistry(root netip.Prefix, registry SystemPoolRegistry) error {
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() || validateRunnerPoolReservation(registry) != nil {
		return corruptSystemPoolRegistry()
	}
	runnerPool, err := ipam.ParseIPv4Prefix(registry.RunnerNetworkPool)
	if err != nil || runnerPool.String() != registry.RunnerNetworkPool || runnerPool.Bits() <= root.Bits() ||
		runnerPool.Bits() > runnerSubnetBits || !root.Contains(runnerPool.Addr()) {
		return corruptSystemPoolRegistry()
	}
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	for owner, value := range registry.Reservations {
		prefix, parseErr := ipam.ParseIPv4Prefix(value)
		if strings.TrimSpace(owner) == "" || parseErr != nil || prefix.String() != value ||
			ipam.ValidateChild(root, prefix, reserved) != nil {
			return corruptSystemPoolRegistry()
		}
		runnerOwner := strings.HasPrefix(owner, "runner/")
		if runnerOwner {
			if ids.Validate(ids.KindRunner, strings.TrimPrefix(owner, "runner/")) != nil ||
				prefix.Bits() != runnerSubnetBits || !runnerPool.Contains(prefix.Addr()) {
				return corruptSystemPoolRegistry()
			}
		} else if prefix.Overlaps(runnerPool) {
			return corruptSystemPoolRegistry()
		}
		reserved = append(reserved, prefix)
	}
	return nil
}

func runnerPoolOwner(runnerID string) string { return "runner/" + runnerID }

func corruptSystemPoolRegistry() error {
	return errs.New(errs.KindInternal, "system pool registry is corrupt")
}

type RunnerTenantQuota struct {
	RunnerIDs []string `json:"runner_ids"`
}

func (quota RunnerTenantQuota) Claim(runnerID string) (RunnerTenantQuota, error) {
	if err := validateRunnerTenantQuota(quota); err != nil {
		return RunnerTenantQuota{}, err
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return RunnerTenantQuota{}, errs.New(errs.KindValidationFailed, "runner id is invalid")
	}
	index := sort.SearchStrings(quota.RunnerIDs, runnerID)
	if index < len(quota.RunnerIDs) && quota.RunnerIDs[index] == runnerID {
		return RunnerTenantQuota{RunnerIDs: append([]string(nil), quota.RunnerIDs...)}, nil
	}
	if len(quota.RunnerIDs) >= maximumTenantRunners {
		return RunnerTenantQuota{}, errs.New(errs.KindResourceInUse, "tenant already owns the maximum of five runners")
	}
	next := RunnerTenantQuota{RunnerIDs: append([]string(nil), quota.RunnerIDs...)}
	next.RunnerIDs = append(next.RunnerIDs, runnerID)
	sort.Strings(next.RunnerIDs)
	return next, nil
}

func (quota RunnerTenantQuota) Release(runnerID string) (RunnerTenantQuota, error) {
	if err := validateRunnerTenantQuota(quota); err != nil {
		return RunnerTenantQuota{}, err
	}
	index := sort.SearchStrings(quota.RunnerIDs, runnerID)
	if index >= len(quota.RunnerIDs) || quota.RunnerIDs[index] != runnerID {
		return RunnerTenantQuota{}, errs.New(
			errs.KindStateConflict,
			"runner tenant quota slot does not match its owner",
		)
	}
	next := RunnerTenantQuota{RunnerIDs: make([]string, 0, len(quota.RunnerIDs)-1)}
	next.RunnerIDs = append(next.RunnerIDs, quota.RunnerIDs[:index]...)
	next.RunnerIDs = append(next.RunnerIDs, quota.RunnerIDs[index+1:]...)
	return next, nil
}

func validateRunnerTenantQuota(quota RunnerTenantQuota) error {
	if len(quota.RunnerIDs) > maximumTenantRunners {
		return corruptRunnerTenantQuota()
	}
	for index, runnerID := range quota.RunnerIDs {
		if ids.Validate(ids.KindRunner, runnerID) != nil ||
			(index > 0 && quota.RunnerIDs[index-1] >= runnerID) {
			return corruptRunnerTenantQuota()
		}
	}
	return nil
}

func corruptRunnerTenantQuota() error {
	return errs.New(errs.KindInternal, "runner tenant quota is corrupt")
}

func runnerTenantQuotaKey(tenantID string) string { return runnerTenantQuotaPrefix + tenantID }
