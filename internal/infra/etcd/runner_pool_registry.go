package etcd

import (
	"fmt"
	"math"
	"net/netip"
	"sort"
	"strconv"
	"strings"

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
	HostPool   RunnerHostPoolConfig
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
	if !config.SystemPool.IsValid() || !config.SystemPool.Addr().Is4() ||
		config.SystemPool != config.SystemPool.Masked() || config.SystemPool.Bits() > runnerSubnetBits {
		return 0, errs.New(errs.KindValidationFailed, "runner system pool must be a canonical IPv4 pool")
	}
	slots, err := config.HostPool.SlotCount()
	if err != nil {
		return 0, err
	}
	networkSlots := uint64(1) << uint(runnerSubnetBits-config.SystemPool.Bits())
	if networkSlots < uint64(slots) {
		return 0, errs.New(errs.KindValidationFailed, "runner system pool is smaller than the host allocation pool")
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
	record, err := decodeEnvelope[RunnerHostSlotRecord](value, "runner_host_slot")
	if err != nil || validateRunnerHostSlotRecord(record) != nil {
		return RunnerHostSlotRecord{}, corruptRunnerHostSlotRecord()
	}
	return record, nil
}

func corruptRunnerHostSlotRecord() error {
	return errs.New(errs.KindInternal, "runner host slot record is corrupt")
}

type SystemPoolRegistry struct {
	Reservations map[string]string `json:"reservations"`
}

func (registry SystemPoolRegistry) ReserveRunner(
	root netip.Prefix,
	runnerID string,
) (SystemPoolRegistry, netip.Prefix, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, errs.New(errs.KindValidationFailed, "runner id is invalid")
	}
	owner := runnerPoolOwner(runnerID)
	reserved, err := registry.prefixes(root)
	if err != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, err
	}
	if value, exists := registry.Reservations[owner]; exists {
		prefix, parseErr := ipam.ParseIPv4Prefix(value)
		if parseErr != nil {
			return SystemPoolRegistry{}, netip.Prefix{}, corruptSystemPoolRegistry()
		}
		next := SystemPoolRegistry{Reservations: make(map[string]string, len(registry.Reservations))}
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
	next := SystemPoolRegistry{Reservations: make(map[string]string, len(registry.Reservations)+1)}
	for key, value := range registry.Reservations {
		next.Reservations[key] = value
	}
	next.Reservations[owner] = candidate.String()
	return next, candidate, nil
}

func (registry SystemPoolRegistry) ReleaseRunner(runnerID string, subnet string) (SystemPoolRegistry, error) {
	owner := runnerPoolOwner(runnerID)
	if registry.Reservations[owner] != subnet {
		return SystemPoolRegistry{}, errs.New(
			errs.KindStateConflict,
			"runner network allocation does not match its owner",
		)
	}
	next := SystemPoolRegistry{Reservations: make(map[string]string, len(registry.Reservations)-1)}
	for key, value := range registry.Reservations {
		if key != owner {
			next.Reservations[key] = value
		}
	}
	return next, nil
}

func (registry SystemPoolRegistry) prefixes(root netip.Prefix) ([]netip.Prefix, error) {
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() {
		return nil, errs.New(errs.KindValidationFailed, "system pool must be a canonical IPv4 pool")
	}
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	for owner, value := range registry.Reservations {
		if strings.TrimSpace(owner) == "" {
			return nil, corruptSystemPoolRegistry()
		}
		prefix, err := ipam.ParseIPv4Prefix(value)
		if err != nil || prefix.String() != value || ipam.ValidateChild(root, prefix, reserved) != nil {
			return nil, corruptSystemPoolRegistry()
		}
		reserved = append(reserved, prefix)
	}
	return reserved, nil
}

func validateSystemPoolRegistry(root netip.Prefix, registry SystemPoolRegistry) error {
	_, err := registry.prefixes(root)
	return err
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
