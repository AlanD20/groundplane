package monitoring

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/common/monitoringwire"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type capturedService struct {
	pageIndex int
	capture   WorkloadCapture
}

type collectionResult struct {
	snapshot     CollectionSnapshot
	measurements []metrics.ServiceMeasurement
	storage      bool
}

// Collect refreshes the shared cache once. It never writes history; Run owns
// closed-bucket persistence so a caller cannot create samples by polling.
func (collector *WorkloadCollector) Collect(ctx context.Context) error {
	_, err := collector.collect(ctx)
	return err
}

func (collector *WorkloadCollector) collect(ctx context.Context) (collectionResult, error) {
	if ctx == nil || collector == nil {
		return collectionResult{}, errs.New(errs.KindInternal, "workload monitoring context is required")
	}
	ctx, cancel := context.WithTimeout(ctx, collector.config.CollectTimeout)
	defer cancel()
	started := collector.now().UTC()
	if started.IsZero() {
		return collectionResult{}, errs.New(errs.KindStorageUnavailable, "workload monitoring clock is unavailable")
	}
	collector.mu.RLock()
	cursor := collector.cursor
	lastStorage := collector.lastStorage
	collector.mu.RUnlock()
	page, err := collector.inventory.Page(ctx, cursor, collector.config.InventoryLimit)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindCursorExpired {
			collector.mu.Lock()
			if collector.cursor == cursor {
				collector.cursor = ""
			}
			collector.mu.Unlock()
		}
		return collectionResult{}, err
	}
	complete := cursor == "" && page.Next == ""
	storageSlot := started.Truncate(collector.config.StorageInterval)
	includeStorage := started.Sub(storageSlot) < metrics.SampleInterval &&
		(lastStorage.IsZero() || lastStorage.Truncate(collector.config.StorageInterval).Before(storageSlot))
	bucketAt := started.Truncate(metrics.SampleInterval)
	samples := make([]ServiceSample, len(page.Items))
	captured := make([]capturedService, 0, len(page.Items))
	for index, item := range page.Items {
		samples[index] = ServiceSample{
			Ownership: item.Ownership, Point: metrics.MetricPoint{At: started}, ContainersComplete: true,
		}
		authority, ok := collector.sources.Capture(ctx, item)
		if !ok {
			continue
		}
		samples[index].ExpectedReplicas = authority.Expected
		samples[index].SourceFingerprint = authority.Fingerprint
		captured = append(captured, capturedService{
			pageIndex: index,
			capture:   WorkloadCapture{Target: item, Source: authority},
		})
	}
	groups := groupCaptured(captured)
	containerCount := 0
	acceptedRaw := make(map[string]metrics.RawContainerSample)
	for _, group := range groups {
		for _, chunk := range splitCaptured(group, includeStorage) {
			rows, volumes, accepted := collector.collectChunk(ctx, chunk, includeStorage)
			if !accepted {
				continue
			}
			for index, item := range chunk {
				sample, raw := collector.serviceSample(started, item.capture, rows[index], volumes)
				if containerCount+len(sample.Containers) > MaximumCachedContainers {
					sample.Containers, sample.ContainersComplete = nil, false
				} else {
					containerCount += len(sample.Containers)
				}
				samples[item.pageIndex] = sample
				for id, value := range raw {
					if _, found := acceptedRaw[id]; found || len(acceptedRaw) < maximumContainerBaselines {
						acceptedRaw[id] = value
					}
				}
			}
		}
	}
	if !includeStorage {
		collector.mu.RLock()
		for index := range samples {
			previous, found := collector.currentService[samples[index].Ownership.ServiceID]
			if !found || !samples[index].Available || !previous.Available ||
				samples[index].Ownership != previous.Ownership ||
				samples[index].SourceFingerprint == (metrics.SourceFingerprint{}) ||
				samples[index].SourceFingerprint != previous.SourceFingerprint || previous.StorageObservedAt.IsZero() ||
				started.Sub(previous.StorageObservedAt) > 2*collector.config.StorageInterval {
				continue
			}
			samples[index].StorageObservedAt = previous.StorageObservedAt
			samples[index].Point.WritableLayerBytes = cloneUint64(previous.Point.WritableLayerBytes)
			samples[index].Point.PersistentVolumeUsedBytes = cloneUint64(previous.Point.PersistentVolumeUsedBytes)
			samples[index].Summaries.WritableLayerBytes = cloneMetricSummary(previous.Summaries.WritableLayerBytes)
			samples[index].Summaries.PersistentVolumeUsedBytes = cloneMetricSummary(
				previous.Summaries.PersistentVolumeUsedBytes,
			)
			samples[index].Volumes = cloneVolumes(previous.Volumes)
		}
		collector.mu.RUnlock()
	}
	snapshot := CollectionSnapshot{
		At: started, InventoryRevision: page.Revision, Complete: complete, Services: samples,
	}
	measurements := make([]metrics.ServiceMeasurement, len(samples))
	for index, sample := range samples {
		point := metrics.ClonePoint(sample.Point)
		volumes := cloneVolumes(sample.Volumes)
		if !includeStorage {
			point.WritableLayerBytes, point.PersistentVolumeUsedBytes = nil, nil
			volumes = nil
		}
		measurements[index] = metrics.ServiceMeasurement{
			Ownership: sample.Ownership, Point: point,
			Bucket:    metrics.Bucket{Start: bucketAt, End: bucketAt.Add(metrics.SampleInterval)},
			Summaries: cloneMetricSummaries(sample.Summaries), Available: sample.Available,
			Volumes: volumes,
		}
	}
	collector.mu.Lock()
	collector.cursor = page.Next
	if includeStorage {
		collector.lastStorage = started
	}
	collector.mergeBaselines(acceptedRaw)
	collector.current = &snapshot
	collector.currentService = make(map[string]ServiceSample, len(samples))
	for _, sample := range samples {
		collector.currentService[sample.Ownership.ServiceID] = sample
	}
	collector.mu.Unlock()
	return collectionResult{
		snapshot: cloneCollectionSnapshot(snapshot), measurements: measurements, storage: includeStorage,
	}, nil
}

func groupCaptured(captured []capturedService) [][]capturedService {
	byEnvironment := make(map[string][]capturedService)
	order := make([]string, 0)
	for _, item := range captured {
		environmentID := item.capture.Target.Ownership.EnvironmentID
		if _, found := byEnvironment[environmentID]; !found {
			order = append(order, environmentID)
		}
		byEnvironment[environmentID] = append(byEnvironment[environmentID], item)
	}
	result := make([][]capturedService, 0, len(order))
	for _, environmentID := range order {
		result = append(result, byEnvironment[environmentID])
	}
	return result
}

func splitCaptured(captured []capturedService, storage bool) [][]capturedService {
	result := make([][]capturedService, 0, 1)
	for len(captured) > 0 {
		end := min(len(captured), monitoringwire.MaximumTargets)
		if storage {
			seen := make(map[string]bool)
			end = 0
			for end < len(captured) && end < monitoringwire.MaximumTargets {
				add := 0
				for _, volume := range captured[end].capture.Source.VolumeTargets {
					if !seen[volume.VolumeId] {
						add++
					}
				}
				if len(seen)+add > monitoringwire.MaximumVolumes {
					break
				}
				for _, volume := range captured[end].capture.Source.VolumeTargets {
					seen[volume.VolumeId] = true
				}
				end++
			}
		}
		if end == 0 {
			// A single Service exceeding the wire Volume bound remains unavailable.
			captured = captured[1:]
			continue
		}
		result = append(result, captured[:end])
		captured = captured[end:]
	}
	return result
}

func (collector *WorkloadCollector) collectChunk(
	ctx context.Context,
	chunk []capturedService,
	storage bool,
) ([]*agentpb.MonitoringServiceSample, map[string]*agentpb.MonitoringVolumeSample, bool) {
	agent, err := collector.agents.GetSingleton(ctx)
	if err != nil || agent.Record.Phase != localagentrecord.LocalAgentPhaseReady || agent.Record.Generation == 0 {
		return nil, nil, false
	}
	targets := make([]*agentpb.ServiceObservationTarget, len(chunk))
	volumes := make([]*agentpb.MonitoringVolumeTarget, 0)
	seenVolumes := make(map[string]bool)
	for index, item := range chunk {
		targets[index] = item.capture.Source.ObservationTarget
		if !storage {
			continue
		}
		for _, volume := range item.capture.Source.VolumeTargets {
			if !seenVolumes[volume.VolumeId] {
				seenVolumes[volume.VolumeId] = true
				volumes = append(volumes, volume)
			}
		}
	}
	result, err := collector.channel.CollectMonitoring(ctx, agent.Record.ID, targets, volumes, storage)
	if err != nil || result == nil || len(result.Services) != len(chunk) {
		return nil, nil, false
	}
	current, err := collector.agents.GetSingleton(ctx)
	if err != nil || current.Revision != agent.Revision || current.Record.ID != agent.Record.ID ||
		current.Record.Generation != agent.Record.Generation || current.Record.Phase != localagentrecord.LocalAgentPhaseReady {
		return nil, nil, false
	}
	recheck := make([]WorkloadCapture, len(chunk))
	for index := range chunk {
		recheck[index] = chunk[index].capture
	}
	fresh := collector.sources.Recheck(ctx, recheck)
	if len(fresh) != len(chunk) {
		return nil, nil, false
	}
	for _, valid := range fresh {
		if !valid {
			return nil, nil, false
		}
	}
	volumeSamples := make(map[string]*agentpb.MonitoringVolumeSample, len(result.Volumes))
	for _, volume := range result.Volumes {
		volumeSamples[volume.VolumeId] = volume
	}
	return result.Services, volumeSamples, true
}

func (collector *WorkloadCollector) serviceSample(
	at time.Time,
	captured WorkloadCapture,
	row *agentpb.MonitoringServiceSample,
	volumeSamples map[string]*agentpb.MonitoringVolumeSample,
) (ServiceSample, map[string]metrics.RawContainerSample) {
	sample := ServiceSample{
		Ownership: captured.Target.Ownership, Point: metrics.MetricPoint{At: at},
		SourceFingerprint: captured.Source.Fingerprint, ExpectedReplicas: captured.Source.Expected,
		ContainersComplete: true,
	}
	if row == nil {
		return sample, nil
	}
	rawSamples := make(map[string]metrics.RawContainerSample, min(len(row.Containers), maximumContainerBaselines))
	containers := make([]ContainerSample, 0, len(row.Containers))
	for _, observed := range row.Containers {
		raw := rawContainer(captured.Source.Fingerprint, observed)
		collector.mu.RLock()
		previous, found := collector.previous[observed.ContainerId]
		collector.mu.RUnlock()
		var prior *metrics.RawContainerSample
		if found {
			prior = &previous
		}
		point, segment, err := metrics.DerivePoint(prior, raw, metrics.MaximumSampleGap)
		if err != nil {
			continue
		}
		if _, found := rawSamples[observed.ContainerId]; found || len(rawSamples) < maximumContainerBaselines {
			rawSamples[observed.ContainerId] = raw
		}
		containers = append(containers, ContainerSample{
			ID: observed.ContainerId, Replica: observed.Replica, RestartCount: observed.RestartCount,
			ObservedAt: raw.At, Point: point, Segment: segment,
		})
	}
	slices.SortFunc(containers, func(left, right ContainerSample) int {
		if left.Replica != right.Replica {
			return int(left.Replica) - int(right.Replica)
		}
		return stringCompare(left.ID, right.ID)
	})
	sample.Containers, sample.ObservedReplicas = containers, uint32(len(containers))
	sample.Available = !row.Unavailable && len(containers) > 0
	sample.Point, sample.Summaries = aggregateContainers(at, sample.ExpectedReplicas, containers)
	for _, target := range captured.Source.VolumeTargets {
		usage := metrics.VolumeUsage{VolumeID: target.VolumeId}
		if observed := volumeSamples[target.VolumeId]; observed != nil {
			usage.UsedBytes = cloneUint64(observed.UsedBytes)
		}
		usage.Summary = instantUintSummary(usage.UsedBytes, 1)
		sample.Volumes = append(sample.Volumes, usage)
	}
	sample.Point.PersistentVolumeUsedBytes, sample.Summaries.PersistentVolumeUsedBytes = aggregateVolumes(sample.Volumes)
	for _, container := range containers {
		if container.Point.WritableLayerBytes != nil && container.ObservedAt.After(sample.StorageObservedAt) {
			sample.StorageObservedAt = container.ObservedAt
		}
	}
	for _, target := range captured.Source.VolumeTargets {
		if observed := volumeSamples[target.VolumeId]; observed != nil && observed.UsedBytes != nil {
			at := time.Unix(0, observed.ObservedAtUnixNano).UTC()
			if at.After(sample.StorageObservedAt) {
				sample.StorageObservedAt = at
			}
		}
	}
	return sample, rawSamples
}

func rawContainer(source metrics.SourceFingerprint, sample *agentpb.MonitoringContainerSample) metrics.RawContainerSample {
	digest := sha256.New()
	_, _ = digest.Write([]byte("groundplane-monitoring-container-source-v1\x00"))
	_, _ = digest.Write(source[:])
	_ = binary.Write(digest, binary.BigEndian, sample.RestartCount)
	var salted metrics.SourceFingerprint
	copy(salted[:], digest.Sum(nil))
	return metrics.RawContainerSample{
		At: time.Unix(0, sample.ObservedAtUnixNano).UTC(), ContainerID: sample.ContainerId, Source: salted,
		CPUTotalNanoseconds: sample.CpuTotalNanoseconds, CPULimitCores: sample.CpuLimitCores,
		MemoryUsageBytes: sample.MemoryUsageBytes, MemoryInactiveFileBytes: sample.MemoryInactiveFileBytes,
		MemoryLimitBytes: sample.MemoryLimitBytes, NetworkReceiveBytes: sample.NetworkReceiveBytes,
		NetworkTransmitBytes: sample.NetworkTransmitBytes, BlockReadBytes: sample.BlockReadBytes,
		BlockWriteBytes: sample.BlockWriteBytes, BlockReadOperations: sample.BlockReadOperations,
		BlockWriteOperations: sample.BlockWriteOperations, WritableLayerBytes: sample.WritableLayerBytes,
	}
}

func (collector *WorkloadCollector) mergeBaselines(accepted map[string]metrics.RawContainerSample) {
	next := make(map[string]metrics.RawContainerSample, maximumContainerBaselines)
	for key, value := range accepted {
		next[key] = value
	}
	keys := make([]string, 0, len(collector.previous))
	for key := range collector.previous {
		if _, current := next[key]; current {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		return collector.previous[keys[left]].At.After(collector.previous[keys[right]].At)
	})
	for _, key := range keys {
		if len(next) == maximumContainerBaselines {
			break
		}
		next[key] = collector.previous[key]
	}
	collector.previous = next
}

func cloneVolumes(volumes []metrics.VolumeUsage) []metrics.VolumeUsage {
	result := make([]metrics.VolumeUsage, len(volumes))
	for index, volume := range volumes {
		result[index] = volume
		result[index].UsedBytes = cloneUint64(volume.UsedBytes)
		result[index].Summary = cloneMetricSummary(volume.Summary)
	}
	return result
}

func stringCompare(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
