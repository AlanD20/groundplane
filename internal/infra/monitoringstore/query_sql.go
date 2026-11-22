package monitoringstore

import "strings"

const serviceSelect = `SELECT bucket_unix, tenant_id, project_id, environment_id, service_id, owner_class,
	sampled_count, eligible_count,
	cpu_sum, cpu_count, cpu_min, cpu_max,
	cpu_limit_sum, cpu_limit_count, cpu_limit_min, cpu_limit_max,
	CAST(memory_sum AS REAL), memory_count, memory_min, memory_max,
	CAST(memory_limit_sum AS REAL), memory_limit_count, memory_limit_min, memory_limit_max,
	network_receive_sum, network_receive_count, network_receive_min, network_receive_max,
	network_transmit_sum, network_transmit_count, network_transmit_min, network_transmit_max,
	CAST(writable_layer_sum AS REAL), writable_layer_count, writable_layer_min, writable_layer_max,
	block_read_sum, block_read_count, block_read_min, block_read_max,
	block_write_sum, block_write_count, block_write_min, block_write_max,
	block_read_operations_sum, block_read_operations_count, block_read_operations_min, block_read_operations_max,
	block_write_operations_sum, block_write_operations_count, block_write_operations_min, block_write_operations_max,
	CAST(volume_sum AS REAL), volume_count, volume_min, volume_max
	FROM monitoring_service_samples`

func scopeSelect() string {
	columns := make([]string, 0, len(scopeMetricColumns)*6)
	for _, metric := range scopeMetricColumns {
		for _, suffix := range []string{"sum", "count", "min", "max", "contributor_valid", "contributor_expected"} {
			columns = append(columns, metric+"_"+suffix)
		}
	}
	return `SELECT bucket_unix, tenant_id, project_id,
		sampled_count, eligible_count, cpu_limited_count, memory_limited_count,
		volume_sampled_count, volume_eligible_count, ` + strings.Join(columns, ",") +
		` FROM monitoring_scope_samples `
}

const hostSelect = `SELECT bucket_unix, host_id, sampled_count, eligible_count,
	cpu_sum, cpu_count, cpu_min, cpu_max,
	cpu_utilization_sum, cpu_utilization_count, cpu_utilization_min, cpu_utilization_max,
	CAST(memory_sum AS REAL), memory_count, memory_min, memory_max,
	CAST(memory_total_sum AS REAL), memory_total_count, memory_total_min, memory_total_max,
	network_receive_sum, network_receive_count, network_receive_min, network_receive_max,
	network_transmit_sum, network_transmit_count, network_transmit_min, network_transmit_max,
	block_read_sum, block_read_count, block_read_min, block_read_max,
	block_write_sum, block_write_count, block_write_min, block_write_max,
	block_read_operations_sum, block_read_operations_count, block_read_operations_min, block_read_operations_max,
	block_write_operations_sum, block_write_operations_count, block_write_operations_min, block_write_operations_max,
	CAST(disk_used_sum AS REAL), disk_used_count, disk_used_min, disk_used_max,
	CAST(disk_total_sum AS REAL), disk_total_count, disk_total_min, disk_total_max
	FROM monitoring_host_samples`
