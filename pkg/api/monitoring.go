package api

import "time"

// MonitoringMetric describes observed samples in one exact bucket. Null
// extrema mean the retained evidence cannot establish them; zero is a value.
type MonitoringMetric struct {
	Average               *float64 `json:"average"`
	Minimum               *float64 `json:"minimum"`
	Maximum               *float64 `json:"maximum"`
	ValidSamples          uint32   `json:"valid_samples"`
	ExpectedSamples       uint32   `json:"expected_samples"`
	ValidContributions    uint32   `json:"valid_contributions"`
	ExpectedContributions uint32   `json:"expected_contributions"`
}

// Units are explicit in field names. Host memory usage and container working
// set, host disk capacity and persistent Volume usage are distinct readings.
type MonitoringValues struct {
	CPUCores                      *MonitoringMetric `json:"cpu_cores,omitempty"`
	CPUUtilizationRatio           *MonitoringMetric `json:"cpu_utilization_ratio,omitempty"`
	CPULimitCores                 *MonitoringMetric `json:"cpu_limit_cores,omitempty"`
	MemoryWorkingSetBytes         *MonitoringMetric `json:"memory_working_set_bytes,omitempty"`
	MemoryUsedBytes               *MonitoringMetric `json:"memory_used_bytes,omitempty"`
	MemoryTotalBytes              *MonitoringMetric `json:"memory_total_bytes,omitempty"`
	MemoryLimitBytes              *MonitoringMetric `json:"memory_limit_bytes,omitempty"`
	NetworkReceiveBytesPerSecond  *MonitoringMetric `json:"network_receive_bytes_per_second,omitempty"`
	NetworkTransmitBytesPerSecond *MonitoringMetric `json:"network_transmit_bytes_per_second,omitempty"`
	WritableLayerBytes            *MonitoringMetric `json:"writable_layer_bytes,omitempty"`
	BlockReadBytesPerSecond       *MonitoringMetric `json:"block_read_bytes_per_second,omitempty"`
	BlockWriteBytesPerSecond      *MonitoringMetric `json:"block_write_bytes_per_second,omitempty"`
	BlockReadOperationsPerSecond  *MonitoringMetric `json:"block_read_operations_per_second,omitempty"`
	BlockWriteOperationsPerSecond *MonitoringMetric `json:"block_write_operations_per_second,omitempty"`
	PersistentVolumeUsedBytes     *MonitoringMetric `json:"persistent_volume_used_bytes,omitempty"`
	DiskUsedBytes                 *MonitoringMetric `json:"disk_used_bytes,omitempty"`
	DiskTotalBytes                *MonitoringMetric `json:"disk_total_bytes,omitempty"`
}

type MonitoringScope struct {
	Kind string `json:"kind" enum:"host,tenant,project,environment,service,backing"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

type MonitoringCoverage struct {
	EligibleResources uint32 `json:"eligible_resources"`
	SampledResources  uint32 `json:"sampled_resources"`
	EligibleVolumes   uint32 `json:"eligible_volumes"`
	SampledVolumes    uint32 `json:"sampled_volumes"`
}

type MonitoringBucket struct {
	Start    time.Time          `json:"start"`
	End      time.Time          `json:"end"`
	Values   MonitoringValues   `json:"values"`
	Coverage MonitoringCoverage `json:"coverage"`
}

type MonitoringWindow struct {
	Range         string    `json:"range" enum:"1h,6h,24h,7d"`
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	BucketSeconds uint32    `json:"bucket_seconds"`
}

type MonitoringLive struct {
	Status     string             `json:"status" enum:"fresh,stale,unavailable"`
	ObservedAt *time.Time         `json:"observed_at"`
	Values     MonitoringValues   `json:"values"`
	Coverage   MonitoringCoverage `json:"coverage"`
}

type MonitoringChild struct {
	Scope        MonitoringScope `json:"scope"`
	Live         MonitoringLive  `json:"live"`
	RuntimeState string          `json:"runtime_state" enum:"unavailable,healthy,running,starting,degraded,stopped,failed,absent"`
}

type MonitoringContainer struct {
	ID           string         `json:"id"`
	Replica      uint32         `json:"replica"`
	RestartCount uint64         `json:"restart_count"`
	Live         MonitoringLive `json:"live"`
}

type MonitoringVolume struct {
	ID         string     `json:"id"`
	ObservedAt *time.Time `json:"observed_at"`
	UsedBytes  *uint64    `json:"used_bytes"`
}

type MonitoringInterface struct {
	Name                   string   `json:"name"`
	Physical               bool     `json:"physical"`
	ReceiveBytesPerSecond  *float64 `json:"receive_bytes_per_second"`
	TransmitBytesPerSecond *float64 `json:"transmit_bytes_per_second"`
}

type MonitoringDevice struct {
	Name                string   `json:"name"`
	Major               uint32   `json:"major"`
	Minor               uint32   `json:"minor"`
	Partition           bool     `json:"partition"`
	RootFilesystem      bool     `json:"root_filesystem"`
	ReadBytesPerSecond  *float64 `json:"read_bytes_per_second"`
	WriteBytesPerSecond *float64 `json:"write_bytes_per_second"`
}

type HostMonitoring struct {
	Live       MonitoringLive        `json:"live"`
	History    []MonitoringBucket    `json:"history"`
	Interfaces []MonitoringInterface `json:"interfaces"`
	Devices    []MonitoringDevice    `json:"devices"`
}

type AggregateMonitoring struct {
	Live     MonitoringLive        `json:"live"`
	History  []MonitoringBucket    `json:"history"`
	Children Page[MonitoringChild] `json:"children"`
}

type ServiceMonitoring struct {
	Live       MonitoringLive            `json:"live"`
	History    []MonitoringBucket        `json:"history"`
	Containers Page[MonitoringContainer] `json:"containers"`
	Volumes    []MonitoringVolume        `json:"volumes"`
}

type MonitoringServiceHealth struct {
	ID                string             `json:"id"`
	Name              string             `json:"name"`
	RuntimeIntent     string             `json:"runtime_intent" enum:"running,stopped,absent"`
	Observation       ServiceObservation `json:"observation"`
	RestartCount      *uint64            `json:"restart_count"`
	RestartObservedAt *time.Time         `json:"restart_observed_at"`
}

type EnvironmentMonitoring struct {
	Services Page[MonitoringServiceHealth] `json:"services"`
}

type BackingMonitoring struct {
	Runtime   ServiceMonitoring     `json:"runtime"`
	Consumers Page[MonitoringScope] `json:"consumers"`
}

// Exactly the payload named by Scope.Kind is populated. Environment monitoring
// is health-only; it never substitutes stored telemetry for live observations.
type MonitoringResponse struct {
	Scope       MonitoringScope        `json:"scope"`
	Window      MonitoringWindow       `json:"window"`
	Host        *HostMonitoring        `json:"host,omitempty"`
	Tenant      *AggregateMonitoring   `json:"tenant,omitempty"`
	Project     *AggregateMonitoring   `json:"project,omitempty"`
	Environment *EnvironmentMonitoring `json:"environment,omitempty"`
	Service     *ServiceMonitoring     `json:"service,omitempty"`
	Backing     *BackingMonitoring     `json:"backing,omitempty"`
}
