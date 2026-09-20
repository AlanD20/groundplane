package scriptrunner

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/blkiodev"
	"github.com/moby/moby/api/types/container"
)

func dockerResources(projection *agentpb.ScriptRunnerProjection) (container.Resources, error) {
	pidsLimit := projection.PidsLimit
	resources := container.Resources{
		CPUShares: projection.CpuShares, Memory: projection.MemLimit,
		CPUPeriod: projection.CpuPeriod, CPUQuota: projection.CpuQuota,
		CPURealtimePeriod: projection.CpuRtPeriod, CPURealtimeRuntime: projection.CpuRtRuntime,
		CpusetCpus: projection.Cpuset, MemoryReservation: projection.MemReservation,
		MemorySwap: projection.MemswapLimit, CPUCount: projection.CpuCount, CPUPercent: int64(projection.CpuPercent),
	}
	if projection.Cpus > 0 {
		resources.NanoCPUs = int64(float64(projection.Cpus) * 1_000_000_000)
	}
	if projection.Limits != nil {
		if resources.NanoCPUs == 0 && projection.Limits.Cpus > 0 {
			resources.NanoCPUs = int64(float64(projection.Limits.Cpus) * 1_000_000_000)
		}
		if resources.Memory == 0 {
			resources.Memory = projection.Limits.MemoryBytes
		}
		if pidsLimit == 0 && projection.Limits.Pids != 0 {
			pidsLimit = projection.Limits.Pids
		}
	}
	if projection.Reservations != nil && resources.MemoryReservation == 0 {
		resources.MemoryReservation = projection.Reservations.MemoryBytes
	}
	if projection.MemSwappiness != 0 {
		resources.MemorySwappiness = int64Pointer(projection.MemSwappiness)
	}
	resources.OomKillDisable = boolPointer(projection.OomKillDisable)
	if pidsLimit != 0 {
		resources.PidsLimit = int64Pointer(pidsLimit)
	}
	for _, limit := range projection.Ulimits {
		if limit == nil {
			return container.Resources{}, errs.New(errs.KindInternal, "Script runner: invalid ulimit")
		}
		soft, hard := limit.Soft, limit.Hard
		if limit.Single != 0 {
			soft, hard = limit.Single, limit.Single
		}
		resources.Ulimits = append(resources.Ulimits, &container.Ulimit{Name: limit.Name, Soft: soft, Hard: hard})
	}
	if projection.Blkio != nil {
		resources.BlkioWeight = uint16(projection.Blkio.Weight)
		for _, value := range projection.Blkio.WeightDevices {
			resources.BlkioWeightDevice = append(
				resources.BlkioWeightDevice,
				&blkiodev.WeightDevice{Path: value.Path, Weight: uint16(value.Weight)},
			)
		}
		var err error
		if resources.BlkioDeviceReadBps, err = throttleDevices(projection.Blkio.DeviceReadBps); err != nil {
			return container.Resources{}, err
		}
		if resources.BlkioDeviceReadIOps, err = throttleDevices(projection.Blkio.DeviceReadIops); err != nil {
			return container.Resources{}, err
		}
		if resources.BlkioDeviceWriteBps, err = throttleDevices(projection.Blkio.DeviceWriteBps); err != nil {
			return container.Resources{}, err
		}
		if resources.BlkioDeviceWriteIOps, err = throttleDevices(projection.Blkio.DeviceWriteIops); err != nil {
			return container.Resources{}, err
		}
	}
	return resources, nil
}

func throttleDevices(values []*agentpb.ScriptThrottleDevice) ([]*blkiodev.ThrottleDevice, error) {
	result := make([]*blkiodev.ThrottleDevice, len(values))
	for index, value := range values {
		if value == nil || value.Rate <= 0 {
			return nil, errs.New(errs.KindInternal, "Script runner: invalid blkio throttle")
		}
		result[index] = &blkiodev.ThrottleDevice{Path: value.Path, Rate: uint64(value.Rate)}
	}
	return result, nil
}
