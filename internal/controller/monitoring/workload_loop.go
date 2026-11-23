package monitoring

import (
	"context"
	"log/slog"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Run owns collection for all viewers and persists at most the latest complete
// aligned reading from each closed 15-second bucket. History failures never
// stop live cache refreshes.
func (collector *WorkloadCollector) Run(ctx context.Context, logger *slog.Logger) error {
	if ctx == nil || collector == nil || logger == nil {
		return errs.New(errs.KindInternal, "workload monitoring loop dependencies are required")
	}
	ticker := time.NewTicker(collector.config.LiveInterval)
	defer ticker.Stop()
	var pending *collectionResult
	for {
		result, err := collector.collect(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.DebugContext(ctx, "workload monitoring sample unavailable", slog.Any("error", err))
		} else {
			bucket := result.snapshot.At.Truncate(metrics.SampleInterval)
			if pending != nil && pending.snapshot.At.Truncate(metrics.SampleInterval).Equal(bucket) {
				preservePendingStorage(*pending, &result)
			}
			if pending != nil && !pending.snapshot.At.Truncate(metrics.SampleInterval).Equal(bucket) {
				collector.appendHistory(ctx, pending.measurements, logger)
				pending = nil
			}
			if result.snapshot.Complete && len(result.measurements) > 0 {
				copy := result
				pending = &copy
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func preservePendingStorage(previous collectionResult, current *collectionResult) {
	prior := make(map[string]int, len(previous.snapshot.Services))
	for index, sample := range previous.snapshot.Services {
		prior[sample.Ownership.ServiceID] = index
	}
	for currentIndex, sample := range current.snapshot.Services {
		previousIndex, found := prior[sample.Ownership.ServiceID]
		if !found || currentIndex >= len(current.measurements) || previousIndex >= len(previous.measurements) {
			continue
		}
		old := previous.snapshot.Services[previousIndex]
		if current.storage || !sample.Available || !old.Available || sample.Ownership != old.Ownership ||
			sample.SourceFingerprint == (metrics.SourceFingerprint{}) || sample.SourceFingerprint != old.SourceFingerprint ||
			old.StorageObservedAt.IsZero() {
			continue
		}
		measurement := &current.measurements[currentIndex]
		oldMeasurement := previous.measurements[previousIndex]
		if sameStorageContainers(sample, old) {
			measurement.Point.WritableLayerBytes = cloneUint64(oldMeasurement.Point.WritableLayerBytes)
			measurement.Summaries.WritableLayerBytes = cloneMetricSummary(oldMeasurement.Summaries.WritableLayerBytes)
		}
		measurement.Point.PersistentVolumeUsedBytes = cloneUint64(oldMeasurement.Point.PersistentVolumeUsedBytes)
		measurement.Summaries.PersistentVolumeUsedBytes = cloneMetricSummary(
			oldMeasurement.Summaries.PersistentVolumeUsedBytes,
		)
		measurement.Volumes = cloneVolumes(oldMeasurement.Volumes)
	}
}

func (collector *WorkloadCollector) appendHistory(
	ctx context.Context,
	measurements []metrics.ServiceMeasurement,
	logger *slog.Logger,
) {
	if collector.history == nil || len(measurements) == 0 {
		return
	}
	write, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := collector.history.AppendBatch(write, measurements)
	cancel()
	if err != nil {
		logger.DebugContext(ctx, "workload monitoring history unavailable", slog.Any("error", err))
	}
}
