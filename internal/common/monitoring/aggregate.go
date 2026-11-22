package monitoring

import (
	"math"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Ownership struct {
	TenantID      string
	ProjectID     string
	EnvironmentID string
	ServiceID     string
	Class         OwnerClass
}

type VolumeUsage struct {
	VolumeID  string
	UsedBytes *uint64
	Summary   MetricSummary
}

type ServiceMeasurement struct {
	Ownership Ownership
	Point     MetricPoint
	Bucket    Bucket
	Summaries MetricSummaries
	Available bool
	Volumes   []VolumeUsage
}

type Coverage struct {
	Eligible       uint32
	Sampled        uint32
	CPULimited     uint32
	MemoryLimited  uint32
	VolumeEligible uint32
	VolumeSampled  uint32
}

type AggregatePoint struct {
	Point     MetricPoint
	Bucket    Bucket
	Summaries MetricSummaries
	Coverage  Coverage
}

func AggregateWorkloads(measurements []ServiceMeasurement) (AggregatePoint, error) {
	var result AggregatePoint
	var at time.Time
	volumes := make(map[string]volumeState)
	for _, measurement := range measurements {
		if err := ValidateMeasurement(measurement); err != nil {
			return AggregatePoint{}, err
		}
		if measurement.Ownership.Class == OwnerBacking {
			continue
		}
		if at.IsZero() {
			at = measurement.Point.At.UTC()
		} else if !measurement.Point.At.Equal(at) {
			return AggregatePoint{}, errs.New(errs.KindValidationFailed, "monitoring aggregate contains different buckets")
		}
		result.Point.At = at
		result.Coverage.Eligible++
		if !measurement.Available {
			continue
		}
		result.Coverage.Sampled++
		sumFloat(&result.Point.CPUCores, measurement.Point.CPUCores)
		sumFloat(&result.Point.CPULimitCores, measurement.Point.CPULimitCores)
		if measurement.Point.CPULimitCores != nil {
			result.Coverage.CPULimited++
		}
		sumUint(&result.Point.MemoryWorkingSetBytes, measurement.Point.MemoryWorkingSetBytes)
		sumUint(&result.Point.MemoryLimitBytes, measurement.Point.MemoryLimitBytes)
		if measurement.Point.MemoryLimitBytes != nil {
			result.Coverage.MemoryLimited++
		}
		sumFloat(&result.Point.NetworkReceiveBytesPerSecond, measurement.Point.NetworkReceiveBytesPerSecond)
		sumFloat(&result.Point.NetworkTransmitBytesPerSecond, measurement.Point.NetworkTransmitBytesPerSecond)
		sumUint(&result.Point.WritableLayerBytes, measurement.Point.WritableLayerBytes)
		sumFloat(&result.Point.BlockReadBytesPerSecond, measurement.Point.BlockReadBytesPerSecond)
		sumFloat(&result.Point.BlockWriteBytesPerSecond, measurement.Point.BlockWriteBytesPerSecond)
		sumFloat(&result.Point.BlockReadOperationsPerSecond, measurement.Point.BlockReadOperationsPerSecond)
		sumFloat(&result.Point.BlockWriteOperationsPerSecond, measurement.Point.BlockWriteOperationsPerSecond)
		for _, volume := range measurement.Volumes {
			if volume.VolumeID == "" {
				return AggregatePoint{}, errs.New(errs.KindValidationFailed, "monitoring volume identity is required")
			}
			state, exists := volumes[volume.VolumeID]
			volumes[volume.VolumeID] = mergeVolume(state, exists, volume.UsedBytes)
		}
	}
	for _, volume := range volumes {
		result.Coverage.VolumeEligible++
		if volume.conflict || volume.used == nil {
			continue
		}
		result.Coverage.VolumeSampled++
		sumUint(&result.Point.PersistentVolumeUsedBytes, volume.used)
	}
	return result, nil
}

func ValidateMeasurement(measurement ServiceMeasurement) error {
	if err := validOwnership(measurement.Ownership); err != nil {
		return err
	}
	if measurement.Point.At.IsZero() {
		return errs.New(errs.KindValidationFailed, "monitoring measurement time is required")
	}
	if err := validateMetricPoint(measurement.Point, false); err != nil {
		return err
	}
	seen := make(map[string]bool, len(measurement.Volumes))
	for _, volume := range measurement.Volumes {
		if volume.VolumeID == "" || seen[volume.VolumeID] {
			return errs.New(errs.KindValidationFailed, "monitoring volume identity is invalid")
		}
		seen[volume.VolumeID] = true
	}
	return nil
}

func validateMetricPoint(point MetricPoint, host bool) error {
	for _, value := range []*float64{
		point.CPUCores,
		point.CPUUtilizationRatio,
		point.CPULimitCores,
		point.NetworkReceiveBytesPerSecond,
		point.NetworkTransmitBytesPerSecond,
		point.BlockReadBytesPerSecond,
		point.BlockWriteBytesPerSecond,
		point.BlockReadOperationsPerSecond,
		point.BlockWriteOperationsPerSecond,
	} {
		if value != nil && (math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0) {
			return errs.New(errs.KindValidationFailed, "monitoring metric is invalid")
		}
	}
	if point.CPUUtilizationRatio != nil && *point.CPUUtilizationRatio > 1 {
		return errs.New(errs.KindValidationFailed, "monitoring CPU utilization is invalid")
	}
	if host && (!validCapacity(point.MemoryUsedBytes, point.MemoryTotalBytes) ||
		!validCapacity(point.DiskUsedBytes, point.DiskTotalBytes)) {
		return errs.New(errs.KindValidationFailed, "monitoring host capacity is invalid")
	}
	return nil
}

type volumeState struct {
	used     *uint64
	conflict bool
}

func mergeVolume(current volumeState, exists bool, value *uint64) volumeState {
	if !exists {
		return volumeState{used: cloneUint(value)}
	}
	if current.conflict || value == nil {
		return current
	}
	if current.used == nil {
		current.used = cloneUint(value)
		return current
	}
	if *current.used != *value {
		current.used, current.conflict = nil, true
	}
	return current
}

func validOwnership(owner Ownership) error {
	if owner.ProjectID == "" || owner.EnvironmentID == "" || owner.ServiceID == "" ||
		(owner.Class != OwnerWorkload && owner.Class != OwnerBacking) ||
		owner.Class == OwnerWorkload && owner.TenantID == "" || owner.Class == OwnerBacking && owner.TenantID != "" {
		return errs.New(errs.KindValidationFailed, "monitoring ownership is invalid")
	}
	return nil
}

func sumFloat(total **float64, value *float64) {
	if value == nil {
		return
	}
	if *total == nil {
		*total = floatPointer(*value)
		return
	}
	**total += *value
}

func sumUint(total **uint64, value *uint64) {
	if value == nil {
		return
	}
	if *total == nil {
		*total = cloneUint(value)
		return
	}
	**total += *value
}
