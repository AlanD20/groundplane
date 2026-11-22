package monitoring

import (
	"math"
	"sort"
	"strconv"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	"github.com/AlanD20/groundplane/pkg/api"
)

const HostLiveFreshness = 15 * time.Second

func PublicHostLive(sample HostSample, now time.Time) api.MonitoringLive {
	if sample.Observed.At.IsZero() {
		return api.MonitoringLive{Status: "unavailable"}
	}
	at := sample.Observed.At.UTC()
	status := "stale"
	if age := now.UTC().Sub(at); !now.IsZero() && age >= 0 && age < HostLiveFreshness {
		status = "fresh"
	}
	return api.MonitoringLive{
		Status: status, ObservedAt: &at, Values: PublicValues(PointSummaries(sample.Point, 1, 1)),
		Coverage: PublicCoverage(metrics.Coverage{Eligible: 1, Sampled: 1}),
	}
}

// PointSummaries exposes only observed values. An unsupported metric remains
// absent rather than becoming a zero-valued sample.
func PointSummaries(point metrics.MetricPoint, eligible, sampled uint32) metrics.MetricSummaries {
	if eligible == 0 || sampled == 0 || sampled > eligible {
		return metrics.MetricSummaries{}
	}
	return metrics.MetricSummaries{
		CPUCores:                      liveFloat(point.CPUCores, eligible, sampled),
		CPUUtilizationRatio:           liveFloat(point.CPUUtilizationRatio, eligible, sampled),
		CPULimitCores:                 liveFloat(point.CPULimitCores, eligible, sampled),
		MemoryWorkingSetBytes:         liveUint(point.MemoryWorkingSetBytes, eligible, sampled),
		MemoryUsedBytes:               liveUint(point.MemoryUsedBytes, eligible, sampled),
		MemoryTotalBytes:              liveUint(point.MemoryTotalBytes, eligible, sampled),
		MemoryLimitBytes:              liveUint(point.MemoryLimitBytes, eligible, sampled),
		NetworkReceiveBytesPerSecond:  liveFloat(point.NetworkReceiveBytesPerSecond, eligible, sampled),
		NetworkTransmitBytesPerSecond: liveFloat(point.NetworkTransmitBytesPerSecond, eligible, sampled),
		WritableLayerBytes:            liveUint(point.WritableLayerBytes, eligible, sampled),
		BlockReadBytesPerSecond:       liveFloat(point.BlockReadBytesPerSecond, eligible, sampled),
		BlockWriteBytesPerSecond:      liveFloat(point.BlockWriteBytesPerSecond, eligible, sampled),
		BlockReadOperationsPerSecond:  liveFloat(point.BlockReadOperationsPerSecond, eligible, sampled),
		BlockWriteOperationsPerSecond: liveFloat(point.BlockWriteOperationsPerSecond, eligible, sampled),
		PersistentVolumeUsedBytes:     liveUint(point.PersistentVolumeUsedBytes, eligible, sampled),
		DiskUsedBytes:                 liveUint(point.DiskUsedBytes, eligible, sampled),
		DiskTotalBytes:                liveUint(point.DiskTotalBytes, eligible, sampled),
	}
}

// AggregateSummaries presents the already-aligned total Point. Child extrema
// are never added: they cannot establish the extrema of a simultaneous total.
// Only per-metric contributor evidence is joined from the child summaries.
func AggregateSummaries(
	point metrics.MetricPoint,
	children []metrics.MetricSummaries,
) metrics.MetricSummaries {
	result := PointSummaries(point, 1, 1)
	entries := []struct {
		target   *metrics.MetricSummary
		selector func(metrics.MetricSummaries) metrics.MetricSummary
	}{
		{&result.CPUCores, func(v metrics.MetricSummaries) metrics.MetricSummary { return v.CPUCores }},
		{
			&result.CPUUtilizationRatio,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.CPUUtilizationRatio },
		},
		{&result.CPULimitCores, func(v metrics.MetricSummaries) metrics.MetricSummary { return v.CPULimitCores }},
		{
			&result.MemoryWorkingSetBytes,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.MemoryWorkingSetBytes },
		},
		{&result.MemoryUsedBytes, func(v metrics.MetricSummaries) metrics.MetricSummary { return v.MemoryUsedBytes }},
		{&result.MemoryTotalBytes, func(v metrics.MetricSummaries) metrics.MetricSummary { return v.MemoryTotalBytes }},
		{&result.MemoryLimitBytes, func(v metrics.MetricSummaries) metrics.MetricSummary { return v.MemoryLimitBytes }},
		{
			&result.NetworkReceiveBytesPerSecond,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.NetworkReceiveBytesPerSecond },
		},
		{
			&result.NetworkTransmitBytesPerSecond,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.NetworkTransmitBytesPerSecond },
		},
		{
			&result.WritableLayerBytes,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.WritableLayerBytes },
		},
		{
			&result.BlockReadBytesPerSecond,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.BlockReadBytesPerSecond },
		},
		{
			&result.BlockWriteBytesPerSecond,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.BlockWriteBytesPerSecond },
		},
		{
			&result.BlockReadOperationsPerSecond,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.BlockReadOperationsPerSecond },
		},
		{
			&result.BlockWriteOperationsPerSecond,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.BlockWriteOperationsPerSecond },
		},
		{
			&result.PersistentVolumeUsedBytes,
			func(v metrics.MetricSummaries) metrics.MetricSummary { return v.PersistentVolumeUsedBytes },
		},
		{&result.DiskUsedBytes, func(v metrics.MetricSummaries) metrics.MetricSummary { return v.DiskUsedBytes }},
		{&result.DiskTotalBytes, func(v metrics.MetricSummaries) metrics.MetricSummary { return v.DiskTotalBytes }},
	}
	for _, entry := range entries {
		if entry.target.Average == nil {
			continue
		}
		var valid, expected uint64
		for _, child := range children {
			value := entry.selector(child)
			valid += uint64(value.ContributorValid)
			expected += uint64(value.ContributorExpected)
		}
		if valid > expected || expected > math.MaxUint32 {
			*entry.target = metrics.MetricSummary{}
			continue
		}
		entry.target.ContributorValid = uint32(valid)
		entry.target.ContributorExpected = uint32(expected)
	}
	return result
}

func HostDetails(sample HostSample) ([]api.MonitoringInterface, []api.MonitoringDevice) {
	return publicInterfaces(sample), publicDevices(sample)
}

func liveFloat(value *float64, eligible, sampled uint32) metrics.MetricSummary {
	if value == nil {
		return metrics.MetricSummary{}
	}
	copy := *value
	return metrics.MetricSummary{
		Average: &copy, Minimum: &copy, Maximum: &copy, Valid: sampled, Expected: eligible,
		ContributorValid: sampled, ContributorExpected: eligible,
	}
}

func liveUint(value *uint64, eligible, sampled uint32) metrics.MetricSummary {
	if value == nil {
		return metrics.MetricSummary{}
	}
	copy := float64(*value)
	return metrics.MetricSummary{
		Average: &copy, Minimum: &copy, Maximum: &copy, Valid: sampled, Expected: eligible,
		ContributorValid: sampled, ContributorExpected: eligible,
	}
}

type interfaceIdentity struct {
	name     string
	physical bool
}

func publicInterfaces(sample HostSample) []api.MonitoringInterface {
	current, currentCount := indexInterfaces(sample.Observed.Interfaces)
	previous, previousCount := map[interfaceIdentity]hoststats.InterfaceUsage{}, map[interfaceIdentity]int{}
	validSegment, elapsed := hostDetailSegment(sample)
	if validSegment {
		previous, previousCount = indexInterfaces(sample.Previous.Interfaces)
	}
	keys := make([]interfaceIdentity, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].name == keys[right].name {
			return keys[left].physical && !keys[right].physical
		}
		return keys[left].name < keys[right].name
	})
	result := make([]api.MonitoringInterface, 0, len(keys))
	for _, key := range keys {
		value := current[key]
		item := api.MonitoringInterface{Name: key.name, Physical: key.physical}
		before, found := previous[key]
		if validSegment && found && currentCount[key] == 1 && previousCount[key] == 1 {
			item.ReceiveBytesPerSecond = counterRate(before.ReceiveBytes, value.ReceiveBytes, elapsed)
			item.TransmitBytesPerSecond = counterRate(before.TransmitBytes, value.TransmitBytes, elapsed)
		}
		result = append(result, item)
	}
	return result
}

func indexInterfaces(
	values []hoststats.InterfaceUsage,
) (map[interfaceIdentity]hoststats.InterfaceUsage, map[interfaceIdentity]int) {
	result := make(map[interfaceIdentity]hoststats.InterfaceUsage)
	counts := make(map[interfaceIdentity]int)
	for _, value := range values {
		if value.Name == "" {
			continue
		}
		key := interfaceIdentity{name: value.Name, physical: value.Physical}
		counts[key]++
		if counts[key] == 1 {
			result[key] = value
		}
	}
	return result, counts
}

func publicDevices(sample HostSample) []api.MonitoringDevice {
	current, currentCount := indexDevices(sample.Observed.Devices)
	previous, previousCount := map[string]hoststats.DeviceUsage{}, map[string]int{}
	validSegment, elapsed := hostDetailSegment(sample)
	if validSegment {
		previous, previousCount = indexDevices(sample.Previous.Devices)
	}
	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]api.MonitoringDevice, 0, len(keys))
	for _, key := range keys {
		value := current[key]
		item := api.MonitoringDevice{
			Name:      value.Name,
			Major:     value.Major,
			Minor:     value.Minor,
			Partition: value.Partition,
			RootFilesystem: value.Major == sample.Observed.RootDeviceMajor &&
				value.Minor == sample.Observed.RootDeviceMinor,
		}
		before, found := previous[key]
		if validSegment && found && currentCount[key] == 1 && previousCount[key] == 1 {
			item.ReadBytesPerSecond = counterRate(before.ReadBytes, value.ReadBytes, elapsed)
			item.WriteBytesPerSecond = counterRate(before.WriteBytes, value.WriteBytes, elapsed)
		}
		result = append(result, item)
	}
	return result
}

func indexDevices(values []hoststats.DeviceUsage) (map[string]hoststats.DeviceUsage, map[string]int) {
	result := make(map[string]hoststats.DeviceUsage)
	counts := make(map[string]int)
	for _, value := range values {
		if value.Name == "" {
			continue
		}
		key := strconv.FormatUint(uint64(value.Major), 10) + ":" + strconv.FormatUint(uint64(value.Minor), 10)
		counts[key]++
		if counts[key] == 1 {
			result[key] = value
		}
	}
	return result, counts
}

func hostDetailSegment(sample HostSample) (bool, float64) {
	if sample.Previous == nil || sample.Observed.BootID == "" || sample.Previous.BootID != sample.Observed.BootID ||
		sample.Previous.RootDeviceMajor != sample.Observed.RootDeviceMajor ||
		sample.Previous.RootDeviceMinor != sample.Observed.RootDeviceMinor {
		return false, 0
	}
	elapsed := sample.Observed.At.Sub(sample.Previous.At)
	if elapsed <= 0 || elapsed > 2*LiveInterval {
		return false, 0
	}
	return true, elapsed.Seconds()
}

func counterRate(previous, current uint64, seconds float64) *float64 {
	if current < previous || seconds <= 0 {
		return nil
	}
	value := float64(current-previous) / seconds
	return &value
}
