package monitoringstore

import (
	"context"
	"database/sql"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type scopeKey struct {
	tenantID  string
	projectID string
}

func (store *Store) appendScope(
	ctx context.Context,
	tx *sql.Tx,
	scope scopeKey,
	aggregate monitoring.AggregatePoint,
	members []monitoring.ServiceMeasurement,
) error {
	values, err := storedScopeMetricValues(aggregate, members)
	if err != nil {
		return err
	}
	arguments := []any{
		int64(
			monitoring.SampleInterval / time.Second,
		), aggregate.Point.At.UTC().Truncate(monitoring.SampleInterval).Unix(),
		scope.tenantID, scope.projectID,
		int64(aggregate.Coverage.Sampled), int64(aggregate.Coverage.Eligible),
		int64(aggregate.Coverage.CPULimited), int64(aggregate.Coverage.MemoryLimited),
		int64(aggregate.Coverage.VolumeSampled), int64(aggregate.Coverage.VolumeEligible),
	}
	arguments = append(arguments, values...)
	if _, err = tx.ExecContext(ctx, upsertScopeSampleSQL(), arguments...); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return nil
}

func storedScopeMetricValues(
	aggregate monitoring.AggregatePoint,
	members []monitoring.ServiceMeasurement,
) ([]any, error) {
	point := aggregate.Point
	expected := uint32(len(members))
	validFloat := func(selectValue func(monitoring.MetricPoint) *float64) uint32 {
		var count uint32
		for _, member := range members {
			if member.Available && selectValue(member.Point) != nil {
				count++
			}
		}
		return count
	}
	validUint := func(selectValue func(monitoring.MetricPoint) *uint64) uint32 {
		var count uint32
		for _, member := range members {
			if member.Available && selectValue(member.Point) != nil {
				count++
			}
		}
		return count
	}
	values := make([]any, 0, len(scopeMetricColumns)*6)
	appendFloat := func(value *float64, valid, eligible uint32) error {
		if value == nil {
			values = append(values, float64(0), int64(0), nil, nil, int64(valid), int64(eligible))
			return nil
		}
		if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 {
			return errs.New(errs.KindValidationFailed, "monitoring scope metric is invalid")
		}
		values = append(values, *value, int64(1), *value, *value, int64(valid), int64(eligible))
		return nil
	}
	appendUint := func(value *uint64, valid, eligible uint32) error {
		stored, count, valueErr := storedUint(value)
		if valueErr != nil {
			return valueErr
		}
		minimum, maximum := nullableStoredUint(value)
		values = append(values, stored, count, minimum, maximum, int64(valid), int64(eligible))
		return nil
	}
	floatMetrics := []struct {
		value       *float64
		selectValue func(monitoring.MetricPoint) *float64
	}{
		{point.CPUCores, func(value monitoring.MetricPoint) *float64 { return value.CPUCores }},
		{point.CPULimitCores, func(value monitoring.MetricPoint) *float64 { return value.CPULimitCores }},
	}
	for _, metric := range floatMetrics {
		if err := appendFloat(metric.value, validFloat(metric.selectValue), expected); err != nil {
			return nil, err
		}
	}
	uintMetrics := []struct {
		value       *uint64
		selectValue func(monitoring.MetricPoint) *uint64
	}{
		{
			point.MemoryWorkingSetBytes,
			func(value monitoring.MetricPoint) *uint64 { return value.MemoryWorkingSetBytes },
		},
		{point.MemoryLimitBytes, func(value monitoring.MetricPoint) *uint64 { return value.MemoryLimitBytes }},
	}
	for _, metric := range uintMetrics {
		if err := appendUint(metric.value, validUint(metric.selectValue), expected); err != nil {
			return nil, err
		}
	}
	remainingFloatMetrics := []struct {
		value       *float64
		selectValue func(monitoring.MetricPoint) *float64
	}{
		{point.NetworkReceiveBytesPerSecond, func(value monitoring.MetricPoint) *float64 {
			return value.NetworkReceiveBytesPerSecond
		}},
		{point.NetworkTransmitBytesPerSecond, func(value monitoring.MetricPoint) *float64 {
			return value.NetworkTransmitBytesPerSecond
		}},
	}
	for _, metric := range remainingFloatMetrics {
		if err := appendFloat(metric.value, validFloat(metric.selectValue), expected); err != nil {
			return nil, err
		}
	}
	if err := appendUint(point.WritableLayerBytes, validUint(func(value monitoring.MetricPoint) *uint64 {
		return value.WritableLayerBytes
	}), expected); err != nil {
		return nil, err
	}
	ioMetrics := []struct {
		value       *float64
		selectValue func(monitoring.MetricPoint) *float64
	}{
		{
			point.BlockReadBytesPerSecond,
			func(value monitoring.MetricPoint) *float64 { return value.BlockReadBytesPerSecond },
		},
		{
			point.BlockWriteBytesPerSecond,
			func(value monitoring.MetricPoint) *float64 { return value.BlockWriteBytesPerSecond },
		},
		{point.BlockReadOperationsPerSecond, func(value monitoring.MetricPoint) *float64 {
			return value.BlockReadOperationsPerSecond
		}},
		{point.BlockWriteOperationsPerSecond, func(value monitoring.MetricPoint) *float64 {
			return value.BlockWriteOperationsPerSecond
		}},
	}
	for _, metric := range ioMetrics {
		if err := appendFloat(metric.value, validFloat(metric.selectValue), expected); err != nil {
			return nil, err
		}
	}
	if err := appendUint(
		point.PersistentVolumeUsedBytes,
		aggregate.Coverage.VolumeSampled,
		aggregate.Coverage.VolumeEligible,
	); err != nil {
		return nil, err
	}
	return values, nil
}
