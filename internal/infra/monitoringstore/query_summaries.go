package monitoringstore

import (
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
)

func metricSummary(value metricValue, expected uint32) monitoring.MetricSummary {
	result := monitoring.MetricSummary{Expected: expected}
	if value.contributorValid >= 0 && value.contributorExpected >= value.contributorValid &&
		value.contributorExpected <= math.MaxUint32 {
		result.ContributorValid = uint32(value.contributorValid)
		result.ContributorExpected = uint32(value.contributorExpected)
	}
	if value.count <= 0 || !value.minimum.Valid || !value.maximum.Valid || value.count > math.MaxUint32 {
		return result
	}
	average := value.sum / float64(value.count)
	minimum, maximum := value.minimum.Float64, value.maximum.Float64
	result.Average, result.Minimum, result.Maximum = &average, &minimum, &maximum
	result.Valid = uint32(value.count)
	return result
}

func scopeSummaries(values []metricValue, expected uint32) monitoring.MetricSummaries {
	result := serviceSummaries(values[:11], expected)
	result.PersistentVolumeUsedBytes = metricSummary(values[11], expected)
	return result
}

func expectedSamples(resolution time.Duration) uint32 {
	return uint32(resolution / monitoring.SampleInterval)
}

func serviceSummaries(values []metricValue, expected uint32) monitoring.MetricSummaries {
	return monitoring.MetricSummaries{
		CPUCores: metricSummary(values[0], expected), CPULimitCores: metricSummary(values[1], expected),
		MemoryWorkingSetBytes: metricSummary(values[2], expected), MemoryLimitBytes: metricSummary(values[3], expected),
		NetworkReceiveBytesPerSecond:  metricSummary(values[4], expected),
		NetworkTransmitBytesPerSecond: metricSummary(values[5], expected),
		WritableLayerBytes:            metricSummary(values[6], expected),
		BlockReadBytesPerSecond:       metricSummary(values[7], expected),
		BlockWriteBytesPerSecond:      metricSummary(values[8], expected),
		BlockReadOperationsPerSecond:  metricSummary(values[9], expected),
		BlockWriteOperationsPerSecond: metricSummary(values[10], expected),
	}
}

func hostSummaries(values []metricValue, expected uint32) monitoring.MetricSummaries {
	return monitoring.MetricSummaries{
		CPUCores: metricSummary(values[0], expected), CPUUtilizationRatio: metricSummary(values[1], expected),
		MemoryUsedBytes: metricSummary(values[2], expected), MemoryTotalBytes: metricSummary(values[3], expected),
		NetworkReceiveBytesPerSecond:  metricSummary(values[4], expected),
		NetworkTransmitBytesPerSecond: metricSummary(values[5], expected),
		BlockReadBytesPerSecond:       metricSummary(values[6], expected),
		BlockWriteBytesPerSecond:      metricSummary(values[7], expected),
		BlockReadOperationsPerSecond:  metricSummary(values[8], expected),
		BlockWriteOperationsPerSecond: metricSummary(values[9], expected),
		DiskUsedBytes:                 metricSummary(values[10], expected),
		DiskTotalBytes:                metricSummary(values[11], expected),
	}
}
