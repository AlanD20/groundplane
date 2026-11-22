package monitoringstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type MaintenanceResult struct {
	RolledServiceRows int64
	RolledVolumeRows  int64
	RolledHostRows    int64
	RolledScopeRows   int64
	ExpiredRows       int64
}

func (store *Store) Maintain(ctx context.Context, now time.Time) (MaintenanceResult, error) {
	if err := ctx.Err(); err != nil {
		return MaintenanceResult{}, err
	}
	if store == nil || store.database == nil || now.IsZero() {
		return MaintenanceResult{}, errs.New(errs.KindInternal, "monitoring maintenance input is invalid")
	}
	tx, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer func() { _ = tx.Rollback() }()
	var result MaintenanceResult
	for _, tier := range []struct {
		source time.Duration
		target time.Duration
	}{
		{monitoring.SampleInterval, monitoring.MediumResolution},
		{monitoring.MediumResolution, monitoring.CoarseResolution},
	} {
		// Publish only after one additional target-sized settlement window.
		// The primary key is immutable after that first complete publication,
		// so source-tier expiry cannot replace it with a partial aggregate.
		cutoff := now.UTC().Truncate(tier.target).Add(-tier.target).Unix()
		serviceResult, executeErr := tx.ExecContext(
			ctx,
			serviceRollupSQL(),
			int64(tier.target/time.Second), int64(tier.target/time.Second),
			int64(tier.source/time.Second), cutoff, int64(tier.target/time.Second),
		)
		if executeErr != nil {
			return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, executeErr)
		}
		volumeResult, executeErr := tx.ExecContext(
			ctx,
			volumeRollupSQL,
			int64(tier.target/time.Second), int64(tier.target/time.Second),
			int64(tier.source/time.Second), cutoff, int64(tier.target/time.Second),
		)
		if executeErr != nil {
			return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, executeErr)
		}
		hostResult, executeErr := tx.ExecContext(
			ctx,
			hostRollupSQL(),
			int64(tier.target/time.Second), int64(tier.target/time.Second),
			int64(tier.source/time.Second), cutoff, int64(tier.target/time.Second),
		)
		if executeErr != nil {
			return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, executeErr)
		}
		scopeResult, executeErr := tx.ExecContext(
			ctx,
			scopeRollupSQL(),
			int64(tier.target/time.Second), int64(tier.target/time.Second),
			int64(tier.source/time.Second), cutoff, int64(tier.target/time.Second),
		)
		if executeErr != nil {
			return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, executeErr)
		}
		serviceRows, rowsErr := rowsAffected(serviceResult)
		if rowsErr != nil {
			return MaintenanceResult{}, rowsErr
		}
		volumeRows, rowsErr := rowsAffected(volumeResult)
		if rowsErr != nil {
			return MaintenanceResult{}, rowsErr
		}
		hostRows, rowsErr := rowsAffected(hostResult)
		if rowsErr != nil {
			return MaintenanceResult{}, rowsErr
		}
		scopeRows, rowsErr := rowsAffected(scopeResult)
		if rowsErr != nil {
			return MaintenanceResult{}, rowsErr
		}
		result.RolledServiceRows += serviceRows
		result.RolledVolumeRows += volumeRows
		result.RolledHostRows += hostRows
		result.RolledScopeRows += scopeRows
	}
	for _, expiry := range []struct {
		resolution time.Duration
		retention  time.Duration
	}{
		{monitoring.SampleInterval, monitoring.FineRetention},
		{monitoring.MediumResolution, monitoring.MediumRetention},
		{monitoring.CoarseResolution, monitoring.CoarseRetention},
	} {
		cutoff := now.UTC().Add(-expiry.retention).Truncate(expiry.resolution).Unix()
		for _, table := range []string{
			"monitoring_volume_samples",
			"monitoring_service_samples",
			"monitoring_host_samples",
			"monitoring_scope_samples",
		} {
			expired, executeErr := tx.ExecContext(
				ctx,
				`DELETE FROM `+table+` WHERE tier_seconds = ? AND bucket_unix < ?`,
				int64(expiry.resolution/time.Second), cutoff,
			)
			if executeErr != nil {
				return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, executeErr)
			}
			expiredRows, rowsErr := rowsAffected(expired)
			if rowsErr != nil {
				return MaintenanceResult{}, rowsErr
			}
			result.ExpiredRows += expiredRows
		}
	}
	if err = tx.Commit(); err != nil {
		return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if _, err = store.database.ExecContext(ctx, `PRAGMA incremental_vacuum(256)`); err != nil {
		return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if _, err = store.database.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return MaintenanceResult{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if err = secureFiles(store.path); err != nil {
		return MaintenanceResult{}, err
	}
	return result, nil
}

func scopeRollupSQL() string {
	columns := []string{
		"sampled_count", "eligible_count", "cpu_limited_count", "memory_limited_count",
		"volume_sampled_count", "volume_eligible_count",
	}
	for _, metric := range scopeMetricColumns {
		for _, suffix := range []string{"sum", "count", "min", "max", "contributor_valid", "contributor_expected"} {
			columns = append(columns, metric+"_"+suffix)
		}
	}
	return fmt.Sprintf(`INSERT INTO monitoring_scope_samples (
		tier_seconds, bucket_unix, tenant_id, project_id, %s
	) SELECT ?, bucket_unix - (bucket_unix %% ?), tenant_id, project_id, %s
	FROM monitoring_scope_samples WHERE tier_seconds = ? AND bucket_unix < ?
	GROUP BY bucket_unix - (bucket_unix %% ?), tenant_id, project_id
	ON CONFLICT(tier_seconds, bucket_unix, tenant_id, project_id) DO NOTHING`,
		strings.Join(columns, ", "), strings.Join(rollupExpressions(columns), ", "))
}

func hostRollupSQL() string {
	columns := []string{
		"sampled_count", "eligible_count",
		"cpu_sum", "cpu_count", "cpu_min", "cpu_max",
		"cpu_utilization_sum", "cpu_utilization_count", "cpu_utilization_min", "cpu_utilization_max",
		"memory_sum", "memory_count", "memory_min", "memory_max",
		"memory_total_sum", "memory_total_count", "memory_total_min", "memory_total_max",
		"network_receive_sum", "network_receive_count", "network_receive_min", "network_receive_max",
		"network_transmit_sum", "network_transmit_count", "network_transmit_min", "network_transmit_max",
		"block_read_sum", "block_read_count", "block_read_min", "block_read_max",
		"block_write_sum", "block_write_count", "block_write_min", "block_write_max",
		"block_read_operations_sum", "block_read_operations_count",
		"block_read_operations_min", "block_read_operations_max",
		"block_write_operations_sum", "block_write_operations_count",
		"block_write_operations_min", "block_write_operations_max",
		"disk_used_sum", "disk_used_count", "disk_used_min", "disk_used_max",
		"disk_total_sum", "disk_total_count", "disk_total_min", "disk_total_max",
	}
	selected := rollupExpressions(columns)
	return fmt.Sprintf(`INSERT INTO monitoring_host_samples (
		tier_seconds, bucket_unix, host_id, %s
	) SELECT ?, bucket_unix - (bucket_unix %% ?), host_id, %s
	FROM monitoring_host_samples WHERE tier_seconds = ? AND bucket_unix < ?
	GROUP BY bucket_unix - (bucket_unix %% ?), host_id
	ON CONFLICT(tier_seconds, bucket_unix, host_id) DO NOTHING`,
		strings.Join(columns, ", "), strings.Join(selected, ", "))
}

func serviceRollupSQL() string {
	columns := []string{
		"sampled_count", "eligible_count",
		"cpu_sum", "cpu_count", "cpu_min", "cpu_max",
		"cpu_limit_sum", "cpu_limit_count", "cpu_limit_min", "cpu_limit_max",
		"memory_sum", "memory_count", "memory_min", "memory_max",
		"memory_limit_sum", "memory_limit_count", "memory_limit_min", "memory_limit_max",
		"network_receive_sum", "network_receive_count", "network_receive_min", "network_receive_max",
		"network_transmit_sum", "network_transmit_count", "network_transmit_min", "network_transmit_max",
		"writable_layer_sum", "writable_layer_count", "writable_layer_min", "writable_layer_max",
		"block_read_sum", "block_read_count", "block_read_min", "block_read_max",
		"block_write_sum", "block_write_count", "block_write_min", "block_write_max",
		"block_read_operations_sum", "block_read_operations_count",
		"block_read_operations_min", "block_read_operations_max",
		"block_write_operations_sum", "block_write_operations_count",
		"block_write_operations_min", "block_write_operations_max",
		"volume_sum", "volume_count", "volume_min", "volume_max",
	}
	selected := rollupExpressions(columns)
	return fmt.Sprintf(`INSERT INTO monitoring_service_samples (
		tier_seconds, bucket_unix, tenant_id, project_id, environment_id, service_id, owner_class, %s
	) SELECT ?, bucket_unix - (bucket_unix %% ?), tenant_id, project_id, environment_id, service_id,
		owner_class, %s
	FROM monitoring_service_samples WHERE tier_seconds = ? AND bucket_unix < ?
	GROUP BY bucket_unix - (bucket_unix %% ?), tenant_id, project_id, environment_id, service_id, owner_class
	ON CONFLICT(tier_seconds, bucket_unix, service_id) DO NOTHING`,
		strings.Join(columns, ", "), strings.Join(selected, ", "))
}

const volumeRollupSQL = `INSERT INTO monitoring_volume_samples (
	tier_seconds, bucket_unix, tenant_id, project_id, environment_id, service_id, owner_class,
	volume_id, used_sum, used_count, used_min, used_max
) SELECT ?, bucket_unix - (bucket_unix % ?), tenant_id, project_id, environment_id, service_id,
	owner_class, volume_id, SUM(used_sum), SUM(used_count), MIN(used_min), MAX(used_max)
FROM monitoring_volume_samples WHERE tier_seconds = ? AND bucket_unix < ?
GROUP BY bucket_unix - (bucket_unix % ?), tenant_id, project_id, environment_id, service_id, owner_class, volume_id
ON CONFLICT(tier_seconds, bucket_unix, service_id, volume_id) DO NOTHING`

func rollupExpressions(columns []string) []string {
	selected := make([]string, len(columns))
	for index, column := range columns {
		function := "SUM"
		if strings.HasSuffix(column, "_min") {
			function = "MIN"
		} else if strings.HasSuffix(column, "_max") {
			function = "MAX"
		}
		selected[index] = function + "(" + column + ")"
	}
	return selected
}

func rowsAffected(result interface{ RowsAffected() (int64, error) }) (int64, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return rows, nil
}
