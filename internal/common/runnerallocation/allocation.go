package runnerallocation

import (
	"math"
	"net/netip"

	commonconfig "github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	RunnerSubnetBits           = 29
	MaximumTenantRunners       = 5
	RunnerSubordinateBlockSize = uint32(65536)
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
	if hostSlots < MaximumTenantRunners || hostSlots > math.MaxUint32 ||
		subUIDValues != hostSlots*uint64(RunnerSubordinateBlockSize) ||
		subGIDValues != hostSlots*uint64(RunnerSubordinateBlockSize) {
		return 0, errs.New(
			errs.KindValidationFailed,
			"runner host allocation ranges must define equal fixed-size slots",
		)
	}
	return uint32(hostSlots), nil
}

func (config RunnerAllocationConfig) Validate() (uint32, error) {
	if !config.SystemPool.IsValid() ||
		!config.SystemPool.Addr().Is4() ||
		config.SystemPool != config.SystemPool.Masked() ||
		!config.RunnerPool.IsValid() ||
		!config.RunnerPool.Addr().Is4() ||
		config.RunnerPool != config.RunnerPool.Masked() ||
		config.RunnerPool.Bits() <= config.SystemPool.Bits() ||
		config.RunnerPool.Bits() > RunnerSubnetBits ||
		!config.SystemPool.Contains(config.RunnerPool.Addr()) {
		return 0, errs.New(
			errs.KindValidationFailed,
			"runner network pool must be a canonical IPv4 child of the system pool",
		)
	}
	slots, err := config.HostPool.SlotCount()
	if err != nil {
		return 0, err
	}
	networkSlots := uint64(1) << uint(RunnerSubnetBits-config.RunnerPool.Bits())
	if networkSlots < uint64(slots) {
		return 0, errs.New(errs.KindValidationFailed, "runner network pool is smaller than the host allocation pool")
	}
	return slots, nil
}

func (config RunnerAllocationConfig) ValidateAllocation(allocation RunnerHostAllocationRecord) error {
	slots, err := config.Validate()
	if err != nil {
		return err
	}
	if allocation.Slot >= slots || allocation.HostUID == 0 || allocation.SubUIDStart == 0 || allocation.SubGIDStart == 0 {
		return errs.New(errs.KindValidationFailed, "runner allocation is outside its configured slot pool")
	}
	prefix, err := ipam.ParseIPv4Prefix(allocation.NetworkCIDR)
	if err != nil || prefix.Bits() != RunnerSubnetBits || !config.RunnerPool.Contains(prefix.Addr()) {
		return errs.New(errs.KindValidationFailed, "runner allocation subnet is outside the runner pool")
	}
	expected, err := config.HostPool.Allocation(allocation.Slot, prefix)
	if err != nil || expected != allocation {
		return errs.New(errs.KindValidationFailed, "runner allocation does not match its configured slot")
	}
	return nil
}

func (config RunnerHostPoolConfig) Allocation(
	slot uint32,
	network netip.Prefix,
) (RunnerHostAllocationRecord, error) {
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
		SubUIDStart: uint32(uint64(config.SubUIDStart) + uint64(slot)*uint64(RunnerSubordinateBlockSize)),
		SubUIDCount: RunnerSubordinateBlockSize,
		SubGIDStart: uint32(uint64(config.SubGIDStart) + uint64(slot)*uint64(RunnerSubordinateBlockSize)),
		SubGIDCount: RunnerSubordinateBlockSize,
		NetworkCIDR: network.String(),
	}
	if err := allocation.Validate(); err != nil {
		return RunnerHostAllocationRecord{}, err
	}
	return allocation, nil
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

func (allocation RunnerHostAllocationRecord) Validate() error {
	prefix, err := ipam.ParseIPv4Prefix(allocation.NetworkCIDR)
	if err != nil || prefix.String() != allocation.NetworkCIDR || prefix.Bits() != RunnerSubnetBits ||
		allocation.SubUIDCount != RunnerSubordinateBlockSize ||
		allocation.SubGIDCount != RunnerSubordinateBlockSize {
		return errs.New(errs.KindValidationFailed, "runner allocation is invalid")
	}
	return nil
}
