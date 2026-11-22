package monitoringstore

import (
	"context"
	"database/sql"
	"math"
	"os"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumStoredPoints      = int(monitoring.CoarseRetention / monitoring.CoarseResolution)
	maximumServiceVolumeRows = maximumStoredPoints * 16
)

type AggregateScope struct {
	TenantID  string
	ProjectID string
}

type Usage struct {
	DatabaseBytes        int64
	WALBytes             int64
	MaximumDatabaseBytes int64
	WALTargetBytes       int64
}

func (store *Store) ReadHost(
	ctx context.Context,
	hostID string,
	start time.Time,
	end time.Time,
	resolution time.Duration,
) ([]monitoring.HostMeasurement, error) {
	if err := store.validateQuery(ctx, start, end, resolution); err != nil {
		return nil, err
	}
	if hostID == "" {
		return nil, errs.New(errs.KindValidationFailed, "monitoring Host identity is required")
	}
	rows, err := store.database.QueryContext(ctx, hostSelect+`
		WHERE tier_seconds = ? AND host_id = ? AND bucket_unix >= ? AND bucket_unix < ?
		ORDER BY bucket_unix`, int64(resolution/time.Second), hostID, start.Unix(), end.Unix())
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer rows.Close()
	result := make([]monitoring.HostMeasurement, 0)
	for rows.Next() {
		var bucket, sampled, eligible int64
		var storedHostID string
		values := make([]metricValue, 12)
		destinations := []any{&bucket, &storedHostID, &sampled, &eligible}
		for index := range values {
			destinations = append(destinations, &values[index].sum, &values[index].count,
				&values[index].minimum, &values[index].maximum)
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, errs.Wrap(errs.KindStorageUnavailable, err)
		}
		point, pointErr := pointFromHostValues(time.Unix(bucket, 0).UTC(), values)
		if pointErr != nil {
			return nil, pointErr
		}
		if storedHostID != hostID || sampled < 0 || eligible <= 0 || eligible > math.MaxUint32 || sampled > eligible {
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring Host row is invalid")
		}
		result = append(result, monitoring.HostMeasurement{
			HostID: storedHostID, Point: point,
			Bucket:    monitoring.Bucket{Start: point.At, End: point.At.Add(resolution)},
			Summaries: hostSummaries(values, expectedSamples(resolution)), Available: sampled > 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return result, nil
}

func (store *Store) ReadService(
	ctx context.Context,
	serviceID string,
	start time.Time,
	end time.Time,
	resolution time.Duration,
) ([]monitoring.ServiceMeasurement, error) {
	if err := store.validateQuery(ctx, start, end, resolution); err != nil {
		return nil, err
	}
	if serviceID == "" {
		return nil, errs.New(errs.KindValidationFailed, "monitoring Service identity is required")
	}
	rows, err := store.database.QueryContext(ctx, serviceSelect+`
		WHERE tier_seconds = ? AND service_id = ? AND bucket_unix >= ? AND bucket_unix < ?
		ORDER BY bucket_unix LIMIT ?`, int64(resolution/time.Second), serviceID, start.Unix(), end.Unix(), maximumStoredPoints+1)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	measurements, err := scanMeasurements(rows, maximumStoredPoints, resolution)
	if err != nil {
		return nil, err
	}
	if err := store.attachVolumes(ctx, measurements, `service_id = ?`, []any{serviceID}, start, end, resolution); err != nil {
		return nil, err
	}
	return orderedMeasurements(measurements), nil
}

func (store *Store) ReadWorkloadAggregate(
	ctx context.Context,
	scope AggregateScope,
	start time.Time,
	end time.Time,
	resolution time.Duration,
) ([]monitoring.AggregatePoint, error) {
	if err := store.validateQuery(ctx, start, end, resolution); err != nil {
		return nil, err
	}
	if scope.TenantID == "" {
		return nil, errs.New(errs.KindValidationFailed, "monitoring aggregate Tenant identity is required")
	}
	rows, err := store.database.QueryContext(ctx, scopeSelect()+`
		WHERE tier_seconds = ? AND tenant_id = ? AND project_id = ?
		AND bucket_unix >= ? AND bucket_unix < ? ORDER BY bucket_unix LIMIT ?`,
		int64(resolution/time.Second), scope.TenantID, scope.ProjectID,
		start.Unix(), end.Unix(), maximumStoredPoints+1)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return scanScopeAggregates(rows, scope, maximumStoredPoints, resolution)
}

func scanScopeAggregates(
	rows *sql.Rows,
	scope AggregateScope,
	maximum int,
	resolution time.Duration,
) ([]monitoring.AggregatePoint, error) {
	defer rows.Close()
	result := make([]monitoring.AggregatePoint, 0)
	for rows.Next() {
		if len(result) >= maximum {
			return nil, errs.New(errs.KindResourceInUse, "monitoring aggregate query point limit exceeded")
		}
		var bucket, eligible, sampled, cpuLimited, memoryLimited, volumeSampled, volumeEligible int64
		var tenantID, projectID string
		values := make([]metricValue, 12)
		destinations := []any{
			&bucket, &tenantID, &projectID, &sampled, &eligible, &cpuLimited, &memoryLimited,
			&volumeSampled, &volumeEligible,
		}
		for index := range values {
			destinations = append(destinations,
				&values[index].sum, &values[index].count, &values[index].minimum, &values[index].maximum,
				&values[index].contributorValid, &values[index].contributorExpected,
			)
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, errs.Wrap(errs.KindStorageUnavailable, err)
		}
		for _, value := range values {
			if value.contributorValid < 0 || value.contributorExpected < value.contributorValid ||
				value.contributorExpected > math.MaxUint32 {
				return nil, errs.New(errs.KindStorageUnavailable, "monitoring scope metric coverage is invalid")
			}
		}
		point, err := pointFromScopeValues(time.Unix(bucket, 0).UTC(), values)
		if err != nil || eligible <= 0 || eligible > math.MaxUint32 || sampled < 0 || sampled > eligible ||
			cpuLimited < 0 ||
			cpuLimited > eligible ||
			memoryLimited < 0 ||
			memoryLimited > eligible || volumeEligible < 0 || volumeSampled < 0 ||
			volumeSampled > volumeEligible || tenantID != scope.TenantID || projectID != scope.ProjectID {
			if err != nil {
				return nil, err
			}
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring aggregate row is invalid")
		}
		result = append(result, monitoring.AggregatePoint{
			Point: point, Bucket: monitoring.Bucket{Start: point.At, End: point.At.Add(resolution)},
			Summaries: scopeSummaries(values, expectedSamples(resolution)),
			Coverage: monitoring.Coverage{
				Eligible: uint32(eligible), Sampled: uint32(sampled),
				CPULimited: uint32(cpuLimited), MemoryLimited: uint32(memoryLimited),
				VolumeEligible: uint32(volumeEligible), VolumeSampled: uint32(volumeSampled),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return result, nil
}

func (store *Store) Usage(ctx context.Context) (Usage, error) {
	if err := ctx.Err(); err != nil {
		return Usage{}, err
	}
	if store == nil || store.database == nil {
		return Usage{}, errs.New(errs.KindInternal, "monitoring store is not configured")
	}
	var pageSize, pageCount int64
	if err := store.database.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return Usage{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if err := store.database.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return Usage{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	walBytes, err := fileSize(store.path + "-wal")
	if err != nil {
		return Usage{}, err
	}
	return Usage{
		DatabaseBytes: pageSize * pageCount, WALBytes: walBytes,
		MaximumDatabaseBytes: store.maximumDatabaseBytes, WALTargetBytes: store.walTargetBytes,
	}, nil
}

func (store *Store) validateQuery(ctx context.Context, start, end time.Time, resolution time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.database == nil {
		return errs.New(errs.KindInternal, "monitoring store is not configured")
	}
	retention, valid := retentionFor(resolution)
	if !valid || start.IsZero() || end.IsZero() || !start.Before(end) || end.Sub(start) > retention ||
		!start.Equal(start.Truncate(resolution)) || !end.Equal(end.Truncate(resolution)) {
		return errs.New(errs.KindValidationFailed, "monitoring query window is invalid")
	}
	return nil
}

func retentionFor(resolution time.Duration) (time.Duration, bool) {
	switch resolution {
	case monitoring.SampleInterval:
		return monitoring.FineRetention, true
	case monitoring.MediumResolution:
		return monitoring.MediumRetention, true
	case monitoring.CoarseResolution:
		return monitoring.CoarseRetention, true
	default:
		return 0, false
	}
}

func scanMeasurements(
	rows *sql.Rows,
	maximum int,
	resolution time.Duration,
) (map[measurementKey]monitoring.ServiceMeasurement, error) {
	defer rows.Close()
	result := make(map[measurementKey]monitoring.ServiceMeasurement)
	for rows.Next() {
		var bucket, sampled, eligible int64
		var owner monitoring.Ownership
		var class string
		values := make([]metricValue, 11)
		destinations := []any{
			&bucket, &owner.TenantID, &owner.ProjectID, &owner.EnvironmentID, &owner.ServiceID, &class,
			&sampled, &eligible,
		}
		for index := range values {
			destinations = append(destinations, &values[index].sum, &values[index].count,
				&values[index].minimum, &values[index].maximum)
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, errs.Wrap(errs.KindStorageUnavailable, err)
		}
		if len(result) >= maximum {
			return nil, errs.New(errs.KindResourceInUse, "monitoring query row limit exceeded")
		}
		owner.Class = monitoring.OwnerClass(class)
		point, err := pointFromValues(time.Unix(bucket, 0).UTC(), values)
		if err != nil || sampled < 0 || eligible <= 0 || eligible > math.MaxUint32 || sampled > eligible {
			if err != nil {
				return nil, err
			}
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring database row is invalid")
		}
		key := measurementKey{bucket: bucket, serviceID: owner.ServiceID}
		result[key] = monitoring.ServiceMeasurement{
			Ownership: owner, Point: point,
			Bucket:    monitoring.Bucket{Start: point.At, End: point.At.Add(resolution)},
			Summaries: serviceSummaries(values, expectedSamples(resolution)), Available: sampled > 0,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return result, nil
}

func (store *Store) attachVolumes(
	ctx context.Context,
	measurements map[measurementKey]monitoring.ServiceMeasurement,
	where string,
	whereArguments []any,
	start time.Time,
	end time.Time,
	resolution time.Duration,
) error {
	arguments := []any{int64(resolution / time.Second)}
	arguments = append(arguments, whereArguments...)
	arguments = append(arguments, start.Unix(), end.Unix())
	rows, err := store.database.QueryContext(ctx, `SELECT bucket_unix, service_id, volume_id,
		used_sum, used_count, used_min, used_max
		FROM monitoring_volume_samples WHERE tier_seconds = ? AND `+where+`
		AND bucket_unix >= ? AND bucket_unix < ? ORDER BY bucket_unix, service_id, volume_id`, arguments...)
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer rows.Close()
	volumeRows := 0
	for rows.Next() {
		volumeRows++
		if volumeRows > maximumServiceVolumeRows {
			return errs.New(errs.KindResourceInUse, "monitoring volume query row limit exceeded")
		}
		var bucket, sum, count int64
		var minimum, maximum sql.NullFloat64
		var serviceID, volumeID string
		if err := rows.Scan(&bucket, &serviceID, &volumeID, &sum, &count, &minimum, &maximum); err != nil {
			return errs.Wrap(errs.KindStorageUnavailable, err)
		}
		key := measurementKey{bucket: bucket, serviceID: serviceID}
		measurement, exists := measurements[key]
		if !exists || volumeID == "" {
			return errs.New(errs.KindStorageUnavailable, "monitoring volume row has no Service sample")
		}
		used, err := averageUint(sum, count)
		if err != nil {
			return err
		}
		measurement.Volumes = append(measurement.Volumes, monitoring.VolumeUsage{
			VolumeID: volumeID, UsedBytes: used,
			Summary: metricSummary(metricValue{sum: float64(sum), count: count, minimum: minimum, maximum: maximum},
				expectedSamples(resolution)),
		})
		measurements[key] = measurement
	}
	if err := rows.Err(); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return nil
}

func orderedMeasurements(values map[measurementKey]monitoring.ServiceMeasurement) []monitoring.ServiceMeasurement {
	result := make([]monitoring.ServiceMeasurement, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Point.At.Before(result[right].Point.At) })
	return result
}

type measurementKey struct {
	bucket    int64
	serviceID string
}

type metricValue struct {
	sum                 float64
	count               int64
	minimum             sql.NullFloat64
	maximum             sql.NullFloat64
	contributorValid    int64
	contributorExpected int64
}

func pointFromValues(at time.Time, values []metricValue) (monitoring.MetricPoint, error) {
	if len(values) != 11 {
		return monitoring.MetricPoint{}, errs.New(errs.KindInternal, "monitoring metric column count is invalid")
	}
	uintValue := func(index int) (*uint64, error) {
		value := values[index]
		if value.sum > float64(^uint64(0)) {
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring integer aggregate is invalid")
		}
		return averageUint(int64(value.sum), value.count)
	}
	memory, err := uintValue(2)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	memoryLimit, err := uintValue(3)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	writable, err := uintValue(6)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	return monitoring.MetricPoint{
		At:       at,
		CPUCores: averageFloat(values[0]), CPULimitCores: averageFloat(values[1]),
		MemoryWorkingSetBytes: memory, MemoryLimitBytes: memoryLimit,
		NetworkReceiveBytesPerSecond:  averageFloat(values[4]),
		NetworkTransmitBytesPerSecond: averageFloat(values[5]),
		WritableLayerBytes:            writable,
		BlockReadBytesPerSecond:       averageFloat(values[7]),
		BlockWriteBytesPerSecond:      averageFloat(values[8]),
		BlockReadOperationsPerSecond:  averageFloat(values[9]),
		BlockWriteOperationsPerSecond: averageFloat(values[10]),
	}, nil
}

func pointFromScopeValues(at time.Time, values []metricValue) (monitoring.MetricPoint, error) {
	if len(values) != 12 {
		return monitoring.MetricPoint{}, errs.New(errs.KindInternal, "monitoring scope column count is invalid")
	}
	point, err := pointFromValues(at, values[:11])
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	point.PersistentVolumeUsedBytes, err = averageUint(int64(values[11].sum), values[11].count)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	return point, nil
}

func pointFromHostValues(at time.Time, values []metricValue) (monitoring.MetricPoint, error) {
	if len(values) != 12 {
		return monitoring.MetricPoint{}, errs.New(errs.KindInternal, "monitoring Host column count is invalid")
	}
	uintValue := func(index int) (*uint64, error) {
		return averageUint(int64(values[index].sum), values[index].count)
	}
	memory, err := uintValue(2)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	memoryTotal, err := uintValue(3)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	disk, err := uintValue(10)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	diskTotal, err := uintValue(11)
	if err != nil {
		return monitoring.MetricPoint{}, err
	}
	return monitoring.MetricPoint{
		At: at, CPUCores: averageFloat(values[0]), CPUUtilizationRatio: averageFloat(values[1]),
		MemoryUsedBytes: memory, MemoryTotalBytes: memoryTotal,
		NetworkReceiveBytesPerSecond:  averageFloat(values[4]),
		NetworkTransmitBytesPerSecond: averageFloat(values[5]),
		BlockReadBytesPerSecond:       averageFloat(values[6]), BlockWriteBytesPerSecond: averageFloat(values[7]),
		BlockReadOperationsPerSecond:  averageFloat(values[8]),
		BlockWriteOperationsPerSecond: averageFloat(values[9]),
		DiskUsedBytes:                 disk, DiskTotalBytes: diskTotal,
	}, nil
}

func averageFloat(value metricValue) *float64 {
	if value.count <= 0 {
		return nil
	}
	result := value.sum / float64(value.count)
	return &result
}

func averageUint(sum, count int64) (*uint64, error) {
	if count <= 0 {
		return nil, nil
	}
	if sum < 0 {
		return nil, errs.New(errs.KindStorageUnavailable, "monitoring integer aggregate is invalid")
	}
	value := uint64(sum / count)
	return &value, nil
}

func fileSize(path string) (int64, error) {
	information, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return information.Size(), nil
}
