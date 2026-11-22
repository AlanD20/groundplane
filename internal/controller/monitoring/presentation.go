package monitoring

import (
	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/api"
)

func publicMetric(value metrics.MetricSummary) *api.MonitoringMetric {
	if value.Expected == 0 && value.Valid == 0 && value.Average == nil && value.Minimum == nil && value.Maximum == nil {
		return nil
	}
	return &api.MonitoringMetric{
		Average:         value.Average,
		Minimum:         value.Minimum,
		Maximum:         value.Maximum,
		ValidSamples:    value.Valid,
		ExpectedSamples: value.Expected,
	}
}

func PublicValues(values metrics.MetricSummaries) api.MonitoringValues {
	return api.MonitoringValues{
		CPUCores: publicMetric(
			values.CPUCores,
		), CPUUtilizationRatio: publicMetric(values.CPUUtilizationRatio), CPULimitCores: publicMetric(values.CPULimitCores),
		MemoryWorkingSetBytes: publicMetric(
			values.MemoryWorkingSetBytes,
		), MemoryUsedBytes: publicMetric(values.MemoryUsedBytes), MemoryTotalBytes: publicMetric(values.MemoryTotalBytes), MemoryLimitBytes: publicMetric(values.MemoryLimitBytes),
		NetworkReceiveBytesPerSecond: publicMetric(
			values.NetworkReceiveBytesPerSecond,
		), NetworkTransmitBytesPerSecond: publicMetric(values.NetworkTransmitBytesPerSecond),
		WritableLayerBytes: publicMetric(
			values.WritableLayerBytes,
		), BlockReadBytesPerSecond: publicMetric(values.BlockReadBytesPerSecond), BlockWriteBytesPerSecond: publicMetric(values.BlockWriteBytesPerSecond),
		BlockReadOperationsPerSecond: publicMetric(
			values.BlockReadOperationsPerSecond,
		), BlockWriteOperationsPerSecond: publicMetric(values.BlockWriteOperationsPerSecond),
		PersistentVolumeUsedBytes: publicMetric(
			values.PersistentVolumeUsedBytes,
		), DiskUsedBytes: publicMetric(values.DiskUsedBytes), DiskTotalBytes: publicMetric(values.DiskTotalBytes),
	}
}

func PublicCoverage(value metrics.Coverage) api.MonitoringCoverage {
	return api.MonitoringCoverage{
		EligibleResources: value.Eligible,
		SampledResources:  value.Sampled,
		EligibleVolumes:   value.VolumeEligible,
		SampledVolumes:    value.VolumeSampled,
	}
}
