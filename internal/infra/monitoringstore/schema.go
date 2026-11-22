package monitoringstore

import "strings"

const serviceSchema = `CREATE TABLE monitoring_service_samples (
	tier_seconds INTEGER NOT NULL, bucket_unix INTEGER NOT NULL,
	tenant_id TEXT NOT NULL, project_id TEXT NOT NULL, environment_id TEXT NOT NULL,
	service_id TEXT NOT NULL, owner_class TEXT NOT NULL CHECK (owner_class IN ('workload', 'backing')),
	sampled_count INTEGER NOT NULL, eligible_count INTEGER NOT NULL,
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
	volume_sum INTEGER NOT NULL, volume_count INTEGER NOT NULL, volume_min INTEGER, volume_max INTEGER,
	PRIMARY KEY (tier_seconds, bucket_unix, service_id)
) STRICT`

const volumeSchema = `CREATE TABLE monitoring_volume_samples (
	tier_seconds INTEGER NOT NULL, bucket_unix INTEGER NOT NULL,
	tenant_id TEXT NOT NULL, project_id TEXT NOT NULL, environment_id TEXT NOT NULL,
	service_id TEXT NOT NULL, owner_class TEXT NOT NULL CHECK (owner_class IN ('workload', 'backing')),
	volume_id TEXT NOT NULL, used_sum INTEGER NOT NULL, used_count INTEGER NOT NULL, used_min INTEGER, used_max INTEGER,
	PRIMARY KEY (tier_seconds, bucket_unix, service_id, volume_id)
) STRICT`

const hostSchema = `CREATE TABLE monitoring_host_samples (
	tier_seconds INTEGER NOT NULL, bucket_unix INTEGER NOT NULL, host_id TEXT NOT NULL,
	sampled_count INTEGER NOT NULL, eligible_count INTEGER NOT NULL,
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
	"writable_layer", "block_read", "block_write", "block_read_operations", "block_write_operations", "volume",
}

var hostMetricColumns = []string{
	"cpu", "cpu_utilization", "memory", "memory_total", "network_receive", "network_transmit",
	"block_read", "block_write", "block_read_operations", "block_write_operations", "disk_used", "disk_total",
}

var scopeMetricColumns = []string{
	"cpu", "cpu_limit", "memory", "memory_limit", "network_receive", "network_transmit",
	"writable_layer", "block_read", "block_write", "block_read_operations", "block_write_operations", "volume",
}

type scopeTable struct {
	name, parentColumn, childColumn string
}

var (
	hierarchyScopeTable   = scopeTable{"monitoring_scope_samples", "tenant_id", "project_id"}
	environmentScopeTable = scopeTable{"monitoring_environment_samples", "project_id", "environment_id"}
)

func (table scopeTable) ownerColumns() string {
	return table.parentColumn + ", " + table.childColumn
}

func scopeSchema(table scopeTable) string {
	columns := make([]string, 0, len(scopeMetricColumns)*6)
	for _, metric := range scopeMetricColumns {
		columns = append(columns,
			metric+"_sum REAL NOT NULL", metric+"_count INTEGER NOT NULL",
			metric+"_min REAL", metric+"_max REAL",
			metric+"_contributor_valid INTEGER NOT NULL", metric+"_contributor_expected INTEGER NOT NULL",
		)
	}
	return `CREATE TABLE IF NOT EXISTS ` + table.name + ` (
		tier_seconds INTEGER NOT NULL, bucket_unix INTEGER NOT NULL,
		` + table.parentColumn + ` TEXT NOT NULL, ` + table.childColumn + ` TEXT NOT NULL,
		sampled_count INTEGER NOT NULL, eligible_count INTEGER NOT NULL,
		cpu_limited_count INTEGER NOT NULL, memory_limited_count INTEGER NOT NULL,
		volume_sampled_count INTEGER NOT NULL, volume_eligible_count INTEGER NOT NULL,
		` + strings.Join(columns, ",") + `,
		PRIMARY KEY (tier_seconds, bucket_unix, ` + table.ownerColumns() + `)
	) STRICT`
}

func upsertServiceSampleSQL() string {
	return upsertSampleSQL("monitoring_service_samples", 9+len(serviceMetricColumns)*4, []string{
		"tenant_id", "project_id", "environment_id", "owner_class", "sampled_count", "eligible_count",
	}, serviceMetricColumns, "tier_seconds, bucket_unix, service_id")
}

func upsertHostSampleSQL() string {
	return upsertSampleSQL("monitoring_host_samples", 5+len(hostMetricColumns)*4,
		[]string{"sampled_count", "eligible_count"}, hostMetricColumns, "tier_seconds, bucket_unix, host_id")
}

func upsertScopeSampleSQL(table scopeTable) string {
	assignments := []string{
		"sampled_count", "eligible_count", "cpu_limited_count", "memory_limited_count",
		"volume_sampled_count", "volume_eligible_count",
	}
	for _, metric := range scopeMetricColumns {
		for _, suffix := range []string{"sum", "count", "min", "max", "contributor_valid", "contributor_expected"} {
			assignments = append(assignments, metric+"_"+suffix)
		}
	}
	set := make([]string, len(assignments))
	for index, column := range assignments {
		set[index] = column + "=excluded." + column
	}
	return `INSERT INTO ` + table.name + ` VALUES (` +
		strings.TrimSuffix(strings.Repeat("?,", 10+len(scopeMetricColumns)*6), ",") +
		`) ON CONFLICT(tier_seconds, bucket_unix, ` + table.ownerColumns() +
		`) DO UPDATE SET ` + strings.Join(set, ",")
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
