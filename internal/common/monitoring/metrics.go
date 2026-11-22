// Package monitoring owns normalized resource measurements, counter segments,
// and parent aggregation. It has no transport or persistence dependency.
package monitoring

import (
	"bytes"
	"math"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	SampleInterval     = 15 * time.Second
	MaximumSampleGap   = 2 * SampleInterval
	FineRetention      = 2 * time.Hour
	MediumResolution   = time.Minute
	MediumRetention    = 24 * time.Hour
	CoarseResolution   = 5 * time.Minute
	CoarseRetention    = 7 * 24 * time.Hour
	MaximumQueryPoints = 720
)

type OwnerClass string

const (
	OwnerWorkload OwnerClass = "workload"
	OwnerBacking  OwnerClass = "backing"
	OwnerHost     OwnerClass = "host"
)

type SourceFingerprint [32]byte

type RawContainerSample struct {
	At                      time.Time
	ContainerID             string
	Source                  SourceFingerprint
	CPUTotalNanoseconds     *uint64
	CPULimitCores           *float64
	MemoryUsageBytes        *uint64
	MemoryInactiveFileBytes *uint64
	MemoryLimitBytes        *uint64
	NetworkReceiveBytes     *uint64
	NetworkTransmitBytes    *uint64
	BlockReadBytes          *uint64
	BlockWriteBytes         *uint64
	BlockReadOperations     *uint64
	BlockWriteOperations    *uint64
	WritableLayerBytes      *uint64
}

type SegmentReason string

const (
	SegmentContinued       SegmentReason = "continued"
	SegmentFirstSample     SegmentReason = "first_sample"
	SegmentContainerChange SegmentReason = "container_changed"
	SegmentSourceChange    SegmentReason = "source_changed"
	SegmentTimeReset       SegmentReason = "time_reset"
	SegmentGap             SegmentReason = "gap"
	SegmentCounterReset    SegmentReason = "counter_reset"
	SegmentCapacityChange  SegmentReason = "capacity_changed"
)

type MetricPoint struct {
	At                            time.Time
	CPUCores                      *float64
	CPUUtilizationRatio           *float64
	CPULimitCores                 *float64
	MemoryWorkingSetBytes         *uint64
	MemoryUsedBytes               *uint64
	MemoryTotalBytes              *uint64
	MemoryLimitBytes              *uint64
	NetworkReceiveBytesPerSecond  *float64
	NetworkTransmitBytesPerSecond *float64
	WritableLayerBytes            *uint64
	BlockReadBytesPerSecond       *float64
	BlockWriteBytesPerSecond      *float64
	BlockReadOperationsPerSecond  *float64
	BlockWriteOperationsPerSecond *float64
	PersistentVolumeUsedBytes     *uint64
	DiskUsedBytes                 *uint64
	DiskTotalBytes                *uint64
}

// Bucket is an exact half-open history interval. Summary values describe only
// samples whose timestamps fall in [Start, End).
type Bucket struct {
	Start time.Time
	End   time.Time
}

type MetricSummary struct {
	Average             *float64
	Minimum             *float64
	Maximum             *float64
	Valid               uint32
	Expected            uint32
	ContributorValid    uint32
	ContributorExpected uint32
}

type MetricSummaries struct {
	CPUCores                      MetricSummary
	CPUUtilizationRatio           MetricSummary
	CPULimitCores                 MetricSummary
	MemoryWorkingSetBytes         MetricSummary
	MemoryUsedBytes               MetricSummary
	MemoryTotalBytes              MetricSummary
	MemoryLimitBytes              MetricSummary
	NetworkReceiveBytesPerSecond  MetricSummary
	NetworkTransmitBytesPerSecond MetricSummary
	WritableLayerBytes            MetricSummary
	BlockReadBytesPerSecond       MetricSummary
	BlockWriteBytesPerSecond      MetricSummary
	BlockReadOperationsPerSecond  MetricSummary
	BlockWriteOperationsPerSecond MetricSummary
	PersistentVolumeUsedBytes     MetricSummary
	DiskUsedBytes                 MetricSummary
	DiskTotalBytes                MetricSummary
}

// ClonePoint detaches every optional metric value from the caller's snapshot.
func ClonePoint(point MetricPoint) MetricPoint {
	return MetricPoint{
		At:                            point.At,
		CPUCores:                      cloneFloat(point.CPUCores),
		CPUUtilizationRatio:           cloneFloat(point.CPUUtilizationRatio),
		CPULimitCores:                 cloneFloat(point.CPULimitCores),
		MemoryWorkingSetBytes:         cloneUint(point.MemoryWorkingSetBytes),
		MemoryUsedBytes:               cloneUint(point.MemoryUsedBytes),
		MemoryTotalBytes:              cloneUint(point.MemoryTotalBytes),
		MemoryLimitBytes:              cloneUint(point.MemoryLimitBytes),
		NetworkReceiveBytesPerSecond:  cloneFloat(point.NetworkReceiveBytesPerSecond),
		NetworkTransmitBytesPerSecond: cloneFloat(point.NetworkTransmitBytesPerSecond),
		WritableLayerBytes:            cloneUint(point.WritableLayerBytes),
		BlockReadBytesPerSecond:       cloneFloat(point.BlockReadBytesPerSecond),
		BlockWriteBytesPerSecond:      cloneFloat(point.BlockWriteBytesPerSecond),
		BlockReadOperationsPerSecond:  cloneFloat(point.BlockReadOperationsPerSecond),
		BlockWriteOperationsPerSecond: cloneFloat(point.BlockWriteOperationsPerSecond),
		PersistentVolumeUsedBytes:     cloneUint(point.PersistentVolumeUsedBytes),
		DiskUsedBytes:                 cloneUint(point.DiskUsedBytes),
		DiskTotalBytes:                cloneUint(point.DiskTotalBytes),
	}
}

func DerivePoint(
	previous *RawContainerSample,
	current RawContainerSample,
	maximumGap time.Duration,
) (MetricPoint, SegmentReason, error) {
	if current.At.IsZero() || current.ContainerID == "" || current.Source == (SourceFingerprint{}) || maximumGap <= 0 {
		return MetricPoint{}, "", errs.New(errs.KindValidationFailed, "monitoring sample identity is invalid")
	}
	if invalidPositiveFloat(current.CPULimitCores) {
		return MetricPoint{}, "", errs.New(errs.KindValidationFailed, "monitoring CPU limit is invalid")
	}
	if (current.NetworkReceiveBytes == nil) != (current.NetworkTransmitBytes == nil) {
		return MetricPoint{}, "", errs.New(errs.KindValidationFailed, "monitoring network counters are incomplete")
	}
	point := MetricPoint{
		At:                    current.At.UTC(),
		CPULimitCores:         cloneFloat(current.CPULimitCores),
		MemoryWorkingSetBytes: workingSet(current.MemoryUsageBytes, current.MemoryInactiveFileBytes),
		MemoryLimitBytes:      positiveUint(current.MemoryLimitBytes),
		WritableLayerBytes:    cloneUint(current.WritableLayerBytes),
	}
	if previous == nil {
		return point, SegmentFirstSample, nil
	}
	if previous.ContainerID != current.ContainerID {
		return point, SegmentContainerChange, nil
	}
	if !bytes.Equal(previous.Source[:], current.Source[:]) {
		return point, SegmentSourceChange, nil
	}
	elapsed := current.At.Sub(previous.At)
	if elapsed <= 0 {
		return point, SegmentTimeReset, nil
	}
	if elapsed > maximumGap {
		return point, SegmentGap, nil
	}
	if counterReset(previous.CPUTotalNanoseconds, current.CPUTotalNanoseconds) ||
		counterReset(previous.NetworkReceiveBytes, current.NetworkReceiveBytes) ||
		counterReset(previous.NetworkTransmitBytes, current.NetworkTransmitBytes) ||
		counterReset(previous.BlockReadBytes, current.BlockReadBytes) ||
		counterReset(previous.BlockWriteBytes, current.BlockWriteBytes) ||
		counterReset(previous.BlockReadOperations, current.BlockReadOperations) ||
		counterReset(previous.BlockWriteOperations, current.BlockWriteOperations) {
		return point, SegmentCounterReset, nil
	}
	seconds := elapsed.Seconds()
	if previous.CPUTotalNanoseconds != nil && current.CPUTotalNanoseconds != nil {
		point.CPUCores = floatPointer(float64(*current.CPUTotalNanoseconds-*previous.CPUTotalNanoseconds) /
			float64(elapsed.Nanoseconds()))
	}
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
	return point, SegmentContinued, nil
}

func workingSet(usage, inactive *uint64) *uint64 {
	if usage == nil || inactive == nil || *inactive > *usage {
		return nil
	}
	value := *usage - *inactive
	return &value
}

func positiveUint(value *uint64) *uint64 {
	if value == nil || *value == 0 {
		return nil
	}
	return cloneUint(value)
}

func invalidPositiveFloat(value *float64) bool {
	return value != nil && (*value <= 0 || math.IsNaN(*value) || math.IsInf(*value, 0))
}

func counterReset(previous, current *uint64) bool {
	return previous != nil && current != nil && *current < *previous
}

func rate(previous, current uint64, seconds float64) *float64 {
	return floatPointer(float64(current-previous) / seconds)
}

func optionalRate(previous, current *uint64, seconds float64) *float64 {
	if previous == nil || current == nil {
		return nil
	}
	return rate(*previous, *current, seconds)
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneUint(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func floatPointer(value float64) *float64 { return &value }
