package monitoring

import (
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// RawHostSample carries kernel counters without assuming a USER_HZ value.
// The busy/total ratio is unit-independent as long as both come from one read.
type RawHostSample struct {
	At                   time.Time
	HostID               string
	Boot                 SourceFingerprint
	LogicalCPUCores      uint32
	CPUTotalTicks        uint64
	CPUBusyTicks         uint64
	MemoryUsedBytes      *uint64
	MemoryTotalBytes     *uint64
	DiskUsedBytes        *uint64
	DiskTotalBytes       *uint64
	NetworkInterfaces    SourceFingerprint
	NetworkReceiveBytes  *uint64
	NetworkTransmitBytes *uint64
	BlockReadBytes       *uint64
	BlockWriteBytes      *uint64
	BlockReadOperations  *uint64
	BlockWriteOperations *uint64
}

type HostMeasurement struct {
	HostID    string
	Point     MetricPoint
	Bucket    Bucket
	Summaries MetricSummaries
	Available bool
}

func DeriveHostPoint(
	previous *RawHostSample,
	current RawHostSample,
	maximumGap time.Duration,
) (MetricPoint, SegmentReason, error) {
	if current.At.IsZero() || current.HostID == "" || current.Boot == (SourceFingerprint{}) ||
		current.LogicalCPUCores == 0 || current.CPUBusyTicks > current.CPUTotalTicks || maximumGap <= 0 {
		return MetricPoint{}, "", errs.New(errs.KindValidationFailed, "monitoring host sample identity is invalid")
	}
	if !validCapacity(current.MemoryUsedBytes, current.MemoryTotalBytes) ||
		!validCapacity(current.DiskUsedBytes, current.DiskTotalBytes) || !validHostNetwork(current) {
		return MetricPoint{}, "", errs.New(errs.KindValidationFailed, "monitoring host capacity is invalid")
	}
	point := MetricPoint{
		At:               current.At.UTC(),
		MemoryUsedBytes:  cloneUint(current.MemoryUsedBytes),
		MemoryTotalBytes: cloneUint(current.MemoryTotalBytes),
		DiskUsedBytes:    cloneUint(current.DiskUsedBytes),
		DiskTotalBytes:   cloneUint(current.DiskTotalBytes),
	}
	if previous == nil {
		return point, SegmentFirstSample, nil
	}
	if previous.HostID != current.HostID || previous.Boot != current.Boot {
		return point, SegmentSourceChange, nil
	}
	if previous.LogicalCPUCores != current.LogicalCPUCores {
		return point, SegmentCapacityChange, nil
	}
	networkSourceChanged := previous.NetworkInterfaces != current.NetworkInterfaces
	elapsed := current.At.Sub(previous.At)
	if elapsed <= 0 {
		return point, SegmentTimeReset, nil
	}
	if elapsed > maximumGap {
		return point, SegmentGap, nil
	}
	if current.CPUTotalTicks < previous.CPUTotalTicks || current.CPUBusyTicks < previous.CPUBusyTicks ||
		!networkSourceChanged && counterReset(previous.NetworkReceiveBytes, current.NetworkReceiveBytes) ||
		!networkSourceChanged && counterReset(previous.NetworkTransmitBytes, current.NetworkTransmitBytes) ||
		counterReset(previous.BlockReadBytes, current.BlockReadBytes) ||
		counterReset(previous.BlockWriteBytes, current.BlockWriteBytes) ||
		counterReset(previous.BlockReadOperations, current.BlockReadOperations) ||
		counterReset(previous.BlockWriteOperations, current.BlockWriteOperations) {
		return point, SegmentCounterReset, nil
	}
	totalDelta := current.CPUTotalTicks - previous.CPUTotalTicks
	busyDelta := current.CPUBusyTicks - previous.CPUBusyTicks
	if totalDelta == 0 || busyDelta > totalDelta {
		return point, SegmentCounterReset, nil
	}
	ratio := float64(busyDelta) / float64(totalDelta)
	point.CPUUtilizationRatio = floatPointer(ratio)
	point.CPUCores = floatPointer(ratio * float64(current.LogicalCPUCores))
	seconds := elapsed.Seconds()
	if !networkSourceChanged {
		point.NetworkReceiveBytesPerSecond = optionalRate(
			previous.NetworkReceiveBytes,
			current.NetworkReceiveBytes,
			seconds,
		)
		point.NetworkTransmitBytesPerSecond = optionalRate(
			previous.NetworkTransmitBytes,
			current.NetworkTransmitBytes,
			seconds,
		)
	}
	point.BlockReadBytesPerSecond = optionalRate(previous.BlockReadBytes, current.BlockReadBytes, seconds)
	point.BlockWriteBytesPerSecond = optionalRate(previous.BlockWriteBytes, current.BlockWriteBytes, seconds)
	point.BlockReadOperationsPerSecond = optionalRate(
		previous.BlockReadOperations,
		current.BlockReadOperations,
		seconds,
	)
	point.BlockWriteOperationsPerSecond = optionalRate(
		previous.BlockWriteOperations,
		current.BlockWriteOperations,
		seconds,
	)
	if networkSourceChanged {
		return point, SegmentSourceChange, nil
	}
	return point, SegmentContinued, nil
}

func ValidateHostMeasurement(measurement HostMeasurement) error {
	if measurement.HostID == "" || measurement.Point.At.IsZero() {
		return errs.New(errs.KindValidationFailed, "monitoring host measurement is invalid")
	}
	return validateMetricPoint(measurement.Point, true)
}

func validHostNetwork(sample RawHostSample) bool {
	if sample.NetworkReceiveBytes == nil && sample.NetworkTransmitBytes == nil {
		return true
	}
	return sample.NetworkReceiveBytes != nil && sample.NetworkTransmitBytes != nil &&
		sample.NetworkInterfaces != (SourceFingerprint{})
}

func validCapacity(used, total *uint64) bool {
	if used == nil && total == nil {
		return true
	}
	return used != nil && total != nil && *total > 0 && *used <= *total
}
