package monitoring

import (
	"context"
	"sync"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/common/monitoringwire"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	DefaultWorkloadStorageInterval = time.Minute
	defaultWorkloadTimeout         = 8 * time.Second
	MaximumCachedContainers        = 4096
	maximumContainerBaselines      = 4096
)

type WorkloadInventory interface {
	Page(context.Context, string, int) (InventoryPage, error)
}

type WorkloadAgents interface {
	GetSingleton(context.Context) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error)
}

type WorkloadChannel interface {
	CollectMonitoring(
		context.Context,
		string,
		[]*agentpb.ServiceObservationTarget,
		[]*agentpb.MonitoringVolumeTarget,
		bool,
	) (*agentpb.MonitoringSamplesResult, error)
}

type WorkloadHistory interface {
	AppendBatch(context.Context, []metrics.ServiceMeasurement) error
}

type WorkloadCapture struct {
	Target InventoryTarget
	Source CapturedWorkload
}

// CapturedWorkload is the transport-safe projection of exact serving authority.
// Authority remains opaque and is used only by WorkloadSourceReader.Recheck.
type CapturedWorkload struct {
	Authority         serviceobservation.MonitoringCapture
	ObservationTarget *agentpb.ServiceObservationTarget
	VolumeTargets     []*agentpb.MonitoringVolumeTarget
	Expected          uint32
	Fingerprint       metrics.SourceFingerprint
}

type WorkloadSources interface {
	Capture(context.Context, InventoryTarget) (CapturedWorkload, bool)
	Recheck(context.Context, []WorkloadCapture) []bool
}

type WorkloadConfig struct {
	LiveInterval    time.Duration
	CollectTimeout  time.Duration
	StorageInterval time.Duration
	InventoryLimit  int
}

func DefaultWorkloadConfig() WorkloadConfig {
	return WorkloadConfig{
		LiveInterval: LiveInterval, CollectTimeout: defaultWorkloadTimeout,
		StorageInterval: DefaultWorkloadStorageInterval, InventoryLimit: MaximumInventoryPage,
	}
}

type ContainerSample struct {
	ID           string
	Replica      uint32
	RestartCount uint64
	ObservedAt   time.Time
	Point        metrics.MetricPoint
	Segment      metrics.SegmentReason
}

type ServiceSample struct {
	Ownership          metrics.Ownership
	Point              metrics.MetricPoint
	Summaries          metrics.MetricSummaries
	Containers         []ContainerSample
	Volumes            []metrics.VolumeUsage
	StorageObservedAt  time.Time
	SourceFingerprint  metrics.SourceFingerprint
	ExpectedReplicas   uint32
	ObservedReplicas   uint32
	ContainersComplete bool
	Available          bool
}

type CollectionSnapshot struct {
	At                time.Time
	InventoryRevision int64
	Complete          bool
	Services          []ServiceSample
}

type WorkloadCollector struct {
	mu sync.RWMutex

	inventory WorkloadInventory
	sources   WorkloadSources
	agents    WorkloadAgents
	channel   WorkloadChannel
	history   WorkloadHistory
	config    WorkloadConfig
	now       func() time.Time

	cursor         string
	lastStorage    time.Time
	previous       map[string]metrics.RawContainerSample
	current        *CollectionSnapshot
	currentService map[string]ServiceSample
}

func NewWorkloadCollector(
	inventory WorkloadInventory,
	sources WorkloadSources,
	agents WorkloadAgents,
	channel WorkloadChannel,
	history WorkloadHistory,
	config WorkloadConfig,
	now func() time.Time,
) (*WorkloadCollector, error) {
	if inventory == nil || sources == nil || agents == nil || channel == nil || now == nil {
		return nil, errs.New(errs.KindInternal, "workload monitoring dependencies are required")
	}
	if config.LiveInterval <= 0 || config.CollectTimeout <= 0 ||
		config.StorageInterval < metrics.SampleInterval || config.StorageInterval%metrics.SampleInterval != 0 ||
		config.InventoryLimit < 1 || config.InventoryLimit > MaximumInventoryPage ||
		config.CollectTimeout > monitoringwire.Timeout {
		return nil, errs.New(errs.KindValidationFailed, "workload monitoring configuration is invalid")
	}
	return &WorkloadCollector{
		inventory: inventory, sources: sources, agents: agents, channel: channel, history: history,
		config: config, now: now, previous: make(map[string]metrics.RawContainerSample),
		currentService: make(map[string]ServiceSample),
	}, nil
}

func (collector *WorkloadCollector) CurrentService(serviceID string) (ServiceSample, bool) {
	if collector == nil || serviceID == "" {
		return ServiceSample{}, false
	}
	collector.mu.RLock()
	defer collector.mu.RUnlock()
	sample, found := collector.currentService[serviceID]
	return cloneServiceSample(sample), found
}

func (collector *WorkloadCollector) Snapshot() (CollectionSnapshot, bool) {
	if collector == nil {
		return CollectionSnapshot{}, false
	}
	collector.mu.RLock()
	defer collector.mu.RUnlock()
	if collector.current == nil {
		return CollectionSnapshot{}, false
	}
	return cloneCollectionSnapshot(*collector.current), true
}

func cloneCollectionSnapshot(snapshot CollectionSnapshot) CollectionSnapshot {
	result := snapshot
	result.Services = make([]ServiceSample, len(snapshot.Services))
	for index, sample := range snapshot.Services {
		result.Services[index] = cloneServiceSample(sample)
	}
	return result
}

func cloneServiceSample(sample ServiceSample) ServiceSample {
	result := sample
	result.Point = metrics.ClonePoint(sample.Point)
	result.Summaries = cloneMetricSummaries(sample.Summaries)
	result.Containers = make([]ContainerSample, len(sample.Containers))
	for index, container := range sample.Containers {
		result.Containers[index] = container
		result.Containers[index].Point = metrics.ClonePoint(container.Point)
	}
	result.Volumes = make([]metrics.VolumeUsage, len(sample.Volumes))
	for index, volume := range sample.Volumes {
		result.Volumes[index] = volume
		result.Volumes[index].UsedBytes = cloneUint64(volume.UsedBytes)
		result.Volumes[index].Summary = cloneMetricSummary(volume.Summary)
	}
	return result
}

func cloneMetricSummaries(value metrics.MetricSummaries) metrics.MetricSummaries {
	result := value
	for _, pair := range [][2]*metrics.MetricSummary{
		{&result.CPUCores, &value.CPUCores}, {&result.CPUUtilizationRatio, &value.CPUUtilizationRatio},
		{&result.CPULimitCores, &value.CPULimitCores},
		{&result.MemoryWorkingSetBytes, &value.MemoryWorkingSetBytes}, {&result.MemoryLimitBytes, &value.MemoryLimitBytes},
		{&result.MemoryUsedBytes, &value.MemoryUsedBytes}, {&result.MemoryTotalBytes, &value.MemoryTotalBytes},
		{&result.NetworkReceiveBytesPerSecond, &value.NetworkReceiveBytesPerSecond},
		{&result.NetworkTransmitBytesPerSecond, &value.NetworkTransmitBytesPerSecond},
		{&result.WritableLayerBytes, &value.WritableLayerBytes},
		{&result.BlockReadBytesPerSecond, &value.BlockReadBytesPerSecond},
		{&result.BlockWriteBytesPerSecond, &value.BlockWriteBytesPerSecond},
		{&result.BlockReadOperationsPerSecond, &value.BlockReadOperationsPerSecond},
		{&result.BlockWriteOperationsPerSecond, &value.BlockWriteOperationsPerSecond},
		{&result.PersistentVolumeUsedBytes, &value.PersistentVolumeUsedBytes},
		{&result.DiskUsedBytes, &value.DiskUsedBytes}, {&result.DiskTotalBytes, &value.DiskTotalBytes},
	} {
		*pair[0] = cloneMetricSummary(*pair[1])
	}
	return result
}

func cloneMetricSummary(value metrics.MetricSummary) metrics.MetricSummary {
	value.Average, value.Minimum, value.Maximum = cloneFloat64(
		value.Average,
	), cloneFloat64(
		value.Minimum,
	), cloneFloat64(
		value.Maximum,
	)
	return value
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
