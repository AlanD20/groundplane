// Package monitoringstore persists bounded, derived monitoring history outside
// the authoritative etcd store.
package monitoringstore

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/errs"
	_ "modernc.org/sqlite"
)

const (
	DefaultMaximumDatabaseBytes int64 = 256 << 20
	DefaultWALTargetBytes       int64 = 16 << 20
	DefaultCacheBytes           int64 = 16 << 20
	schemaVersion                     = 1
	minimumDatabaseBytes        int64 = 64 << 10
	minimumWALTargetBytes       int64 = 32 << 10
)

type Options struct {
	Path                 string
	MaximumDatabaseBytes int64
	WALTargetBytes       int64
	CacheBytes           int64
}

type Store struct {
	database             *sql.DB
	path                 string
	maximumDatabaseBytes int64
	walTargetBytes       int64
}

func Open(ctx context.Context, options Options) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options = defaultOptions(options)
	if !filepath.IsAbs(options.Path) || options.MaximumDatabaseBytes < minimumDatabaseBytes ||
		options.WALTargetBytes < minimumWALTargetBytes || options.CacheBytes <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "monitoring store options are invalid")
	}
	if err := preparePath(options.Path); err != nil {
		return nil, err
	}
	dsn := (&url.URL{Scheme: "file", Path: options.Path}).String()
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	store := &Store{
		database: database, path: options.Path,
		maximumDatabaseBytes: options.MaximumDatabaseBytes,
		walTargetBytes:       options.WALTargetBytes,
	}
	if err := store.configure(ctx, options.CacheBytes); err != nil {
		_ = database.Close()
		return nil, err
	}
	if err := secureFiles(options.Path); err != nil {
		_ = database.Close()
		return nil, err
	}
	return store, nil
}

func (store *Store) Close() error {
	if store == nil || store.database == nil {
		return nil
	}
	if err := store.database.Close(); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return nil
}

func (store *Store) Append(ctx context.Context, measurement monitoring.ServiceMeasurement) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.database == nil {
		return errs.New(errs.KindInternal, "monitoring store is not configured")
	}
	if err := monitoring.ValidateMeasurement(measurement); err != nil {
		return err
	}
	bucket := measurement.Point.At.UTC().Truncate(monitoring.SampleInterval).Unix()
	metricValues, err := storedMetricValues(measurement)
	if err != nil {
		return err
	}
	tx, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer func() { _ = tx.Rollback() }()
	arguments := []any{
		int64(monitoring.SampleInterval / time.Second), bucket,
		measurement.Ownership.TenantID, measurement.Ownership.ProjectID,
		measurement.Ownership.EnvironmentID, measurement.Ownership.ServiceID,
		string(measurement.Ownership.Class), boolCount(measurement.Available), 1,
	}
	arguments = append(arguments, metricValues...)
	if _, err = tx.ExecContext(ctx, upsertServiceSampleSQL(), arguments...); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if _, err = tx.ExecContext(
		ctx,
		`DELETE FROM monitoring_volume_samples WHERE tier_seconds = ? AND bucket_unix = ? AND service_id = ?`,
		int64(monitoring.SampleInterval/time.Second), bucket, measurement.Ownership.ServiceID,
	); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	for _, volume := range measurement.Volumes {
		value, count, valueErr := storedUint(volume.UsedBytes)
		if valueErr != nil {
			return valueErr
		}
		minimum, maximum := nullableStoredUint(volume.UsedBytes)
		if _, err = tx.ExecContext(ctx, insertVolumeSample,
			int64(monitoring.SampleInterval/time.Second), bucket,
			measurement.Ownership.TenantID, measurement.Ownership.ProjectID,
			measurement.Ownership.EnvironmentID, measurement.Ownership.ServiceID,
			string(measurement.Ownership.Class), volume.VolumeID, value, count, minimum, maximum,
		); err != nil {
			return errs.Wrap(errs.KindStorageUnavailable, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if err = secureFiles(store.path); err != nil {
		return err
	}
	return store.checkpointOversizedWAL(ctx)
}

func (store *Store) AppendHost(ctx context.Context, measurement monitoring.HostMeasurement) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.database == nil {
		return errs.New(errs.KindInternal, "monitoring store is not configured")
	}
	if err := monitoring.ValidateHostMeasurement(measurement); err != nil {
		return err
	}
	point := measurement.Point
	if !measurement.Available {
		point = monitoring.MetricPoint{At: point.At}
	}
	values, err := storedHostMetricValues(point)
	if err != nil {
		return err
	}
	arguments := []any{
		int64(monitoring.SampleInterval / time.Second), point.At.UTC().Truncate(monitoring.SampleInterval).Unix(),
		measurement.HostID, boolCount(measurement.Available), 1,
	}
	arguments = append(arguments, values...)
	if _, err = store.database.ExecContext(ctx, upsertHostSampleSQL(), arguments...); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if err = secureFiles(store.path); err != nil {
		return err
	}
	return store.checkpointOversizedWAL(ctx)
}

func defaultOptions(options Options) Options {
	if options.MaximumDatabaseBytes == 0 {
		options.MaximumDatabaseBytes = DefaultMaximumDatabaseBytes
	}
	if options.WALTargetBytes == 0 {
		options.WALTargetBytes = DefaultWALTargetBytes
	}
	if options.CacheBytes == 0 {
		options.CacheBytes = DefaultCacheBytes
	}
	return options
}

func preparePath(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	information, err := os.Lstat(path)
	if err == nil {
		if !information.Mode().IsRegular() {
			return errs.New(errs.KindStorageUnavailable, "monitoring database path is not a regular file")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if err = file.Close(); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return nil
}

func (store *Store) configure(ctx context.Context, cacheBytes int64) error {
	if err := store.database.PingContext(ctx); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	pragmas := []string{
		`PRAGMA busy_timeout = 2000`,
		`PRAGMA foreign_keys = ON`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA journal_mode = WAL`,
		fmt.Sprintf(`PRAGMA cache_size = -%d`, max(1, cacheBytes/1024)),
		fmt.Sprintf(`PRAGMA journal_size_limit = %d`, store.walTargetBytes),
	}
	for _, statement := range pragmas {
		if _, err := store.database.ExecContext(ctx, statement); err != nil {
			return errs.Wrap(errs.KindStorageUnavailable, err)
		}
	}
	var version int
	if err := store.database.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if version != 0 && version != schemaVersion {
		return errs.New(errs.KindStorageUnavailable, "monitoring database schema is unsupported")
	}
	if version == 0 {
		if err := store.createSchema(ctx); err != nil {
			return err
		}
	}
	var pageSize, maximumPages, currentPages int64
	if err := store.database.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if pageSize <= 0 {
		return errs.New(errs.KindStorageUnavailable, "monitoring database page size is invalid")
	}
	requestedPages := max(int64(1), store.maximumDatabaseBytes/pageSize)
	if err := store.database.QueryRowContext(
		ctx,
		fmt.Sprintf(`PRAGMA max_page_count = %d`, requestedPages),
	).Scan(&maximumPages); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if err := store.database.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&currentPages); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if maximumPages > requestedPages || currentPages > requestedPages {
		return errs.New(errs.KindStorageUnavailable, "monitoring database exceeds its configured page ceiling")
	}
	autoCheckpointPages := max(int64(1), store.walTargetBytes/pageSize)
	if _, err := store.database.ExecContext(
		ctx,
		fmt.Sprintf(`PRAGMA wal_autocheckpoint = %d`, autoCheckpointPages),
	); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return nil
}

func (store *Store) createSchema(ctx context.Context) error {
	tx, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer func() { _ = tx.Rollback() }()
	statements := []string{
		`PRAGMA auto_vacuum = INCREMENTAL`,
		serviceSchema,
		volumeSchema,
		hostSchema,
		`CREATE INDEX monitoring_service_scope ON monitoring_service_samples
			(tier_seconds, tenant_id, project_id, bucket_unix, owner_class)`,
		`CREATE INDEX monitoring_volume_scope ON monitoring_volume_samples
			(tier_seconds, tenant_id, project_id, bucket_unix, owner_class, volume_id)`,
		`CREATE INDEX monitoring_host_time ON monitoring_host_samples (tier_seconds, host_id, bucket_unix)`,
		fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion),
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return errs.Wrap(errs.KindStorageUnavailable, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return nil
}

func secureFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Chmod(candidate, 0o600); err != nil && !os.IsNotExist(err) {
			return errs.Wrap(errs.KindStorageUnavailable, err)
		}
	}
	return nil
}

func (store *Store) checkpointOversizedWAL(ctx context.Context) error {
	information, err := os.Stat(store.path + "-wal")
	if os.IsNotExist(err) || err == nil && information.Size() <= store.walTargetBytes {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if _, err = store.database.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return secureFiles(store.path)
}

func storedMetricValues(measurement monitoring.ServiceMeasurement) ([]any, error) {
	point := measurement.Point
	if !measurement.Available {
		point = monitoring.MetricPoint{At: point.At}
	}
	values := make([]any, 0, 44)
	appendFloat := func(value *float64) error {
		if value == nil {
			values = append(values, float64(0), int64(0), nil, nil)
			return nil
		}
		if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 {
			return errs.New(errs.KindValidationFailed, "monitoring metric is invalid")
		}
		values = append(values, *value, int64(1), *value, *value)
		return nil
	}
	appendUint := func(value *uint64) error {
		stored, count, err := storedUint(value)
		if err == nil {
			minimum, maximum := nullableStoredUint(value)
			values = append(values, stored, count, minimum, maximum)
		}
		return err
	}
	for _, appendValue := range []func() error{
		func() error { return appendFloat(point.CPUCores) },
		func() error { return appendFloat(point.CPULimitCores) },
		func() error { return appendUint(point.MemoryWorkingSetBytes) },
		func() error { return appendUint(point.MemoryLimitBytes) },
		func() error { return appendFloat(point.NetworkReceiveBytesPerSecond) },
		func() error { return appendFloat(point.NetworkTransmitBytesPerSecond) },
		func() error { return appendUint(point.WritableLayerBytes) },
		func() error { return appendFloat(point.BlockReadBytesPerSecond) },
		func() error { return appendFloat(point.BlockWriteBytesPerSecond) },
		func() error { return appendFloat(point.BlockReadOperationsPerSecond) },
		func() error { return appendFloat(point.BlockWriteOperationsPerSecond) },
	} {
		if err := appendValue(); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func storedHostMetricValues(point monitoring.MetricPoint) ([]any, error) {
	values := make([]any, 0, 48)
	appendFloat := func(value *float64) error {
		if value == nil {
			values = append(values, float64(0), int64(0), nil, nil)
			return nil
		}
		if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 {
			return errs.New(errs.KindValidationFailed, "monitoring host metric is invalid")
		}
		values = append(values, *value, int64(1), *value, *value)
		return nil
	}
	appendUint := func(value *uint64) error {
		stored, count, err := storedUint(value)
		if err == nil {
			minimum, maximum := nullableStoredUint(value)
			values = append(values, stored, count, minimum, maximum)
		}
		return err
	}
	for _, appendValue := range []func() error{
		func() error { return appendFloat(point.CPUCores) },
		func() error { return appendFloat(point.CPUUtilizationRatio) },
		func() error { return appendUint(point.MemoryUsedBytes) },
		func() error { return appendUint(point.MemoryTotalBytes) },
		func() error { return appendFloat(point.NetworkReceiveBytesPerSecond) },
		func() error { return appendFloat(point.NetworkTransmitBytesPerSecond) },
		func() error { return appendFloat(point.BlockReadBytesPerSecond) },
		func() error { return appendFloat(point.BlockWriteBytesPerSecond) },
		func() error { return appendFloat(point.BlockReadOperationsPerSecond) },
		func() error { return appendFloat(point.BlockWriteOperationsPerSecond) },
		func() error { return appendUint(point.DiskUsedBytes) },
		func() error { return appendUint(point.DiskTotalBytes) },
	} {
		if err := appendValue(); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func storedUint(value *uint64) (int64, int64, error) {
	if value == nil {
		return 0, 0, nil
	}
	if *value > math.MaxInt64 {
		return 0, 0, errs.New(errs.KindValidationFailed, "monitoring metric exceeds the storage range")
	}
	return int64(*value), 1, nil
}

func nullableStoredUint(value *uint64) (any, any) {
	if value == nil {
		return nil, nil
	}
	return int64(*value), int64(*value)
}

func boolCount(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

const serviceSchema = `CREATE TABLE monitoring_service_samples (
	tier_seconds INTEGER NOT NULL,
	bucket_unix INTEGER NOT NULL,
	tenant_id TEXT NOT NULL,
	project_id TEXT NOT NULL,
	environment_id TEXT NOT NULL,
	service_id TEXT NOT NULL,
	owner_class TEXT NOT NULL CHECK (owner_class IN ('workload', 'backing')),
	sampled_count INTEGER NOT NULL,
	eligible_count INTEGER NOT NULL,
	cpu_sum REAL NOT NULL, cpu_count INTEGER NOT NULL, cpu_min REAL, cpu_max REAL,
	cpu_limit_sum REAL NOT NULL, cpu_limit_count INTEGER NOT NULL, cpu_limit_min REAL, cpu_limit_max REAL,
	memory_sum INTEGER NOT NULL, memory_count INTEGER NOT NULL, memory_min INTEGER, memory_max INTEGER,
	memory_limit_sum INTEGER NOT NULL, memory_limit_count INTEGER NOT NULL, memory_limit_min INTEGER, memory_limit_max INTEGER,
	network_receive_sum REAL NOT NULL, network_receive_count INTEGER NOT NULL, network_receive_min REAL, network_receive_max REAL,
	network_transmit_sum REAL NOT NULL, network_transmit_count INTEGER NOT NULL, network_transmit_min REAL, network_transmit_max REAL,
	writable_layer_sum INTEGER NOT NULL, writable_layer_count INTEGER NOT NULL, writable_layer_min INTEGER, writable_layer_max INTEGER,
	block_read_sum REAL NOT NULL, block_read_count INTEGER NOT NULL, block_read_min REAL, block_read_max REAL,
	block_write_sum REAL NOT NULL, block_write_count INTEGER NOT NULL, block_write_min REAL, block_write_max REAL,
	block_read_operations_sum REAL NOT NULL, block_read_operations_count INTEGER NOT NULL,
	block_read_operations_min REAL, block_read_operations_max REAL,
	block_write_operations_sum REAL NOT NULL, block_write_operations_count INTEGER NOT NULL,
	block_write_operations_min REAL, block_write_operations_max REAL,
	PRIMARY KEY (tier_seconds, bucket_unix, service_id)
) STRICT`

const volumeSchema = `CREATE TABLE monitoring_volume_samples (
	tier_seconds INTEGER NOT NULL,
	bucket_unix INTEGER NOT NULL,
	tenant_id TEXT NOT NULL,
	project_id TEXT NOT NULL,
	environment_id TEXT NOT NULL,
	service_id TEXT NOT NULL,
	owner_class TEXT NOT NULL CHECK (owner_class IN ('workload', 'backing')),
	volume_id TEXT NOT NULL,
	used_sum INTEGER NOT NULL,
	used_count INTEGER NOT NULL,
	used_min INTEGER,
	used_max INTEGER,
	PRIMARY KEY (tier_seconds, bucket_unix, service_id, volume_id)
) STRICT`

const hostSchema = `CREATE TABLE monitoring_host_samples (
	tier_seconds INTEGER NOT NULL,
	bucket_unix INTEGER NOT NULL,
	host_id TEXT NOT NULL,
	sampled_count INTEGER NOT NULL,
	eligible_count INTEGER NOT NULL,
	cpu_sum REAL NOT NULL, cpu_count INTEGER NOT NULL, cpu_min REAL, cpu_max REAL,
	cpu_utilization_sum REAL NOT NULL, cpu_utilization_count INTEGER NOT NULL,
	cpu_utilization_min REAL, cpu_utilization_max REAL,
	memory_sum INTEGER NOT NULL, memory_count INTEGER NOT NULL, memory_min INTEGER, memory_max INTEGER,
	memory_total_sum INTEGER NOT NULL, memory_total_count INTEGER NOT NULL, memory_total_min INTEGER, memory_total_max INTEGER,
	network_receive_sum REAL NOT NULL, network_receive_count INTEGER NOT NULL, network_receive_min REAL, network_receive_max REAL,
	network_transmit_sum REAL NOT NULL, network_transmit_count INTEGER NOT NULL, network_transmit_min REAL, network_transmit_max REAL,
	block_read_sum REAL NOT NULL, block_read_count INTEGER NOT NULL, block_read_min REAL, block_read_max REAL,
	block_write_sum REAL NOT NULL, block_write_count INTEGER NOT NULL, block_write_min REAL, block_write_max REAL,
	block_read_operations_sum REAL NOT NULL, block_read_operations_count INTEGER NOT NULL,
	block_read_operations_min REAL, block_read_operations_max REAL,
	block_write_operations_sum REAL NOT NULL, block_write_operations_count INTEGER NOT NULL,
	block_write_operations_min REAL, block_write_operations_max REAL,
	disk_used_sum INTEGER NOT NULL, disk_used_count INTEGER NOT NULL, disk_used_min INTEGER, disk_used_max INTEGER,
	disk_total_sum INTEGER NOT NULL, disk_total_count INTEGER NOT NULL, disk_total_min INTEGER, disk_total_max INTEGER,
	PRIMARY KEY (tier_seconds, bucket_unix, host_id)
) STRICT`

const insertVolumeSample = `INSERT INTO monitoring_volume_samples VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

var serviceMetricColumns = []string{
	"cpu", "cpu_limit", "memory", "memory_limit", "network_receive", "network_transmit",
	"writable_layer", "block_read", "block_write", "block_read_operations", "block_write_operations",
}

var hostMetricColumns = []string{
	"cpu", "cpu_utilization", "memory", "memory_total", "network_receive", "network_transmit",
	"block_read", "block_write", "block_read_operations", "block_write_operations", "disk_used", "disk_total",
}

func upsertServiceSampleSQL() string {
	return upsertSampleSQL("monitoring_service_samples", 9+len(serviceMetricColumns)*4, []string{
		"tenant_id", "project_id", "environment_id", "owner_class", "sampled_count", "eligible_count",
	}, serviceMetricColumns, "tier_seconds, bucket_unix, service_id")
}

func upsertHostSampleSQL() string {
	return upsertSampleSQL(
		"monitoring_host_samples",
		5+len(hostMetricColumns)*4,
		[]string{"sampled_count", "eligible_count"},
		hostMetricColumns,
		"tier_seconds, bucket_unix, host_id",
	)
}

func upsertSampleSQL(table string, valueCount int, assignments, metrics []string, key string) string {
	for _, metric := range metrics {
		for _, suffix := range []string{"sum", "count", "min", "max"} {
			assignments = append(assignments, metric+"_"+suffix)
		}
	}
	set := make([]string, len(assignments))
	for index, column := range assignments {
		set[index] = column + "=excluded." + column
	}
	return `INSERT INTO ` + table + ` VALUES (` + strings.TrimSuffix(strings.Repeat("?,", valueCount), ",") +
		`) ON CONFLICT(` + key + `) DO UPDATE SET ` + strings.Join(set, ",")
}
