package monitoring

import (
	"math"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type historyRow struct {
	Bucket    metrics.Bucket
	Summaries metrics.MetricSummaries
	Coverage  metrics.Coverage
}

type historyFields struct {
	CPU, CPURatio, CPULimit, MemoryWorking, MemoryUsed, MemoryTotal, MemoryLimit bool
	NetworkReceive, NetworkTransmit, WritableLayer                               bool
	BlockRead, BlockWrite, BlockReadOperations, BlockWriteOperations             bool
	PersistentVolume, DiskUsed, DiskTotal                                        bool
}

var hostHistoryFields = historyFields{
	CPU: true, CPURatio: true, MemoryUsed: true, MemoryTotal: true,
	NetworkReceive: true, NetworkTransmit: true, BlockRead: true, BlockWrite: true,
	BlockReadOperations: true, BlockWriteOperations: true, DiskUsed: true, DiskTotal: true,
}

var serviceHistoryFields = historyFields{
	CPU: true, CPURatio: true, CPULimit: true, MemoryWorking: true, MemoryLimit: true,
	NetworkReceive: true, NetworkTransmit: true, WritableLayer: true,
	BlockRead: true, BlockWrite: true, BlockReadOperations: true, BlockWriteOperations: true,
	PersistentVolume: true,
}

var aggregateHistoryFields = func() historyFields {
	fields := serviceHistoryFields
	fields.PersistentVolume = true
	return fields
}()

func groupHistory(window Window, rows []historyRow, fields historyFields) ([]api.MonitoringBucket, error) {
	width := time.Duration(window.GroupSize) * window.SourceResolution
	if width <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "monitoring history bucket width is invalid")
	}
	count := int((window.Public.End.Sub(window.Public.Start) + width - 1) / width)
	if count < 1 || count > metrics.MaximumQueryPoints {
		return nil, errs.New(errs.KindValidationFailed, "monitoring history point count is invalid")
	}
	grouped := make([][]metrics.MetricSummaries, count)
	coverage := make([]metrics.Coverage, count)
	last := make([]time.Time, count)
	previous := time.Time{}
	for _, row := range rows {
		if err := validateHistoryRow(row, window, previous); err != nil {
			return nil, err
		}
		previous = row.Bucket.Start
		index := int(row.Bucket.Start.Sub(window.Public.Start) / width)
		if fields.CPULimit && row.Summaries.CPUUtilizationRatio.Valid == 0 {
			row.Summaries.CPUUtilizationRatio = workloadCPUUtilization(
				row.Summaries.CPUCores, row.Summaries.CPULimitCores,
				uint32(window.SourceResolution/metrics.SampleInterval),
			)
		}
		grouped[index] = append(grouped[index], row.Summaries)
		if last[index].IsZero() || row.Bucket.Start.After(last[index]) {
			coverage[index], last[index] = row.Coverage, row.Bucket.Start
		}
	}
	result := make([]api.MonitoringBucket, count)
	for index := range result {
		start := window.Public.Start.Add(time.Duration(index) * width)
		end := start.Add(width)
		if end.After(window.Public.End) {
			end = window.Public.End
		}
		sourceBuckets := uint64(end.Sub(start) / window.SourceResolution)
		expected := sourceBuckets * uint64(window.SourceResolution/metrics.SampleInterval)
		if expected > math.MaxUint32 {
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring history sample count overflow")
		}
		values, err := combineMetricSummaries(grouped[index])
		if err != nil {
			return nil, err
		}
		storageExpected := cadenceSamples(start, end, DefaultWorkloadStorageInterval)
		if err := applyHistoryExpected(&values, fields, uint32(expected), storageExpected); err != nil {
			return nil, err
		}
		result[index] = api.MonitoringBucket{
			Start: start, End: end, Values: PublicValues(values), Coverage: PublicCoverage(coverage[index]),
		}
	}
	return result, nil
}

func validateHistoryRow(row historyRow, window Window, previous time.Time) error {
	if row.Bucket.Start.Before(window.Public.Start) || !row.Bucket.Start.Before(window.Public.End) ||
		!row.Bucket.Start.Equal(row.Bucket.Start.Truncate(window.SourceResolution)) ||
		!row.Bucket.End.Equal(row.Bucket.Start.Add(window.SourceResolution)) ||
		!previous.IsZero() && !row.Bucket.Start.After(previous) {
		return errs.New(errs.KindStorageUnavailable, "monitoring history row is outside its fixed window")
	}
	return nil
}

func combineMetricSummaries(values []metrics.MetricSummaries) (metrics.MetricSummaries, error) {
	var result metrics.MetricSummaries
	combine := func(selector func(metrics.MetricSummaries) metrics.MetricSummary) (metrics.MetricSummary, error) {
		selected := make([]metrics.MetricSummary, 0, len(values))
		for _, value := range values {
			selected = append(selected, selector(value))
		}
		return CombineMetric(selected)
	}
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
		value, err := combine(entry.selector)
		if err != nil {
			return metrics.MetricSummaries{}, err
		}
		*entry.target = value
	}
	return result, nil
}

func cadenceSamples(start, end time.Time, cadence time.Duration) uint32 {
	first := start.Truncate(cadence)
	if first.Before(start) {
		first = first.Add(cadence)
	}
	if !first.Before(end) {
		return 0
	}
	return uint32(1 + end.Add(-time.Nanosecond).Sub(first)/cadence)
}

func applyHistoryExpected(
	values *metrics.MetricSummaries,
	fields historyFields,
	expected uint32,
	storageExpected uint32,
) error {
	entries := []struct {
		enabled bool
		storage bool
		value   *metrics.MetricSummary
	}{
		{fields.CPU, false, &values.CPUCores}, {fields.CPURatio, false, &values.CPUUtilizationRatio},
		{fields.CPULimit, false, &values.CPULimitCores},
		{fields.MemoryWorking, false, &values.MemoryWorkingSetBytes},
		{fields.MemoryUsed, false, &values.MemoryUsedBytes}, {fields.MemoryTotal, false, &values.MemoryTotalBytes},
		{fields.MemoryLimit, false, &values.MemoryLimitBytes},
		{fields.NetworkReceive, false, &values.NetworkReceiveBytesPerSecond},
		{fields.NetworkTransmit, false, &values.NetworkTransmitBytesPerSecond},
		{fields.WritableLayer, true, &values.WritableLayerBytes},
		{fields.BlockRead, false, &values.BlockReadBytesPerSecond},
		{fields.BlockWrite, false, &values.BlockWriteBytesPerSecond},
		{fields.BlockReadOperations, false, &values.BlockReadOperationsPerSecond},
		{fields.BlockWriteOperations, false, &values.BlockWriteOperationsPerSecond},
		{fields.PersistentVolume, true, &values.PersistentVolumeUsedBytes},
		{fields.DiskUsed, false, &values.DiskUsedBytes}, {fields.DiskTotal, false, &values.DiskTotalBytes},
	}
	for _, entry := range entries {
		if !entry.enabled {
			continue
		}
		metricExpected := expected
		if entry.storage {
			metricExpected = storageExpected
		}
		if entry.value.Valid > metricExpected || !entry.storage && entry.value.Expected > metricExpected {
			return errs.New(errs.KindStorageUnavailable, "monitoring history sample count exceeds its bucket")
		}
		entry.value.Expected = metricExpected
	}
	return nil
}
