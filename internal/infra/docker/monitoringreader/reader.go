// Package monitoringreader samples only exact Controller-selected workloads.
package monitoringreader

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/monitoringwire"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

type Engine interface {
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerStats(context.Context, string, client.ContainerStatsOptions) (client.ContainerStatsResult, error)
	VolumeInspect(context.Context, string, client.VolumeInspectOptions) (client.VolumeInspectResult, error)
}

type Reader struct {
	engine     Engine
	volumeRoot string
	now        func() time.Time
}

func New(engine Engine, volumeRoot string, now func() time.Time) (*Reader, error) {
	if engine == nil || now == nil {
		return nil, errs.New(errs.KindValidationFailed, "monitoring engine and clock are required")
	}
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, err
	}
	return &Reader{engine: engine, volumeRoot: volumeRoot, now: now}, nil
}

func (reader *Reader) Collect(
	ctx context.Context,
	request *agentpb.CollectMonitoringSamples,
) (*agentpb.MonitoringSamplesResult, error) {
	if ctx == nil || reader == nil {
		return nil, errs.New(errs.KindInternal, "monitoring context and reader are required")
	}
	if err := monitoringwire.ValidateRequest(request); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, monitoringwire.Timeout)
	defer cancel()
	listed, err := reader.engine.ContainerList(ctx, client.ContainerListOptions{All: true,
		Limit:   serviceobservation.MaximumContainers + 1,
		Filters: client.Filters{}.Add("label", "com.groundplane.environment-id="+request.Targets[0].EnvironmentId)})
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if len(listed.Items) > serviceobservation.MaximumContainers {
		return nil, errs.New(errs.KindResourceInUse, "monitoring inventory limit exceeded")
	}
	result := &agentpb.MonitoringSamplesResult{
		RequestId: request.RequestId,
		Services:  make([]*agentpb.MonitoringServiceSample, len(request.Targets)),
	}
	var selected atomic.Int32
	var workers sync.WaitGroup
	jobs := make(chan int)
	for range min(16, len(request.Targets)) {
		workers.Go(func() {
			for index := range jobs {
				result.Services[index] = reader.sampleTarget(
					ctx,
					request,
					request.Targets[index],
					listed.Items,
					&selected,
				)
			}
		})
	}
	for index := range request.Targets {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result.Volumes = reader.sampleVolumes(ctx, request)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := monitoringwire.ValidateResult(request, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (reader *Reader) sampleTarget(
	ctx context.Context,
	request *agentpb.CollectMonitoringSamples,
	target *agentpb.ServiceObservationTarget,
	candidates []container.Summary,
	selected *atomic.Int32,
) *agentpb.MonitoringServiceSample {
	row := &agentpb.MonitoringServiceSample{ServiceId: target.ServiceId, ReleaseId: target.ReleaseId, Unavailable: true}
	var samples []*agentpb.MonitoringContainerSample
	ids, replicas := make(map[string]bool), make(map[uint64]bool)
	for _, candidate := range candidates {
		if !serviceobservation.SelectsWorkload(candidate.Labels, target) {
			continue
		}
		replica, valid := serviceobservation.WorkloadReplica(candidate.Labels, target)
		if !valid || candidate.ID == "" || ids[candidate.ID] || replicas[replica] ||
			selected.Add(1) > monitoringwire.MaximumContainers {
			return row
		}
		ids[candidate.ID], replicas[replica] = true, true
		sample := reader.sampleContainer(ctx, request, target, candidate.ID, replica)
		if sample == nil {
			return row
		}
		samples = append(samples, sample)
	}
	if len(samples) > 0 {
		row.Unavailable, row.Containers = false, samples
	}
	return row
}

func (reader *Reader) sampleContainer(ctx context.Context, request *agentpb.CollectMonitoringSamples,
	target *agentpb.ServiceObservationTarget, id string, replica uint64) *agentpb.MonitoringContainerSample {
	before, err := reader.engine.ContainerInspect(ctx, id, client.ContainerInspectOptions{Size: request.IncludeStorage})
	if err != nil || !matches(before.Container, target, id, replica) {
		return nil
	}
	startedAt, err := time.Parse(time.RFC3339Nano, before.Container.State.StartedAt)
	if err != nil || startedAt.UnixNano() <= 0 {
		return nil
	}
	stats, err := reader.engine.ContainerStats(ctx, id, client.ContainerStatsOptions{})
	if err != nil {
		return nil
	}
	if stats.Body == nil {
		return nil
	}
	row, decodeErr := decodeStatistics(stats.Body, id, reader.now())
	closeErr := stats.Body.Close()
	if decodeErr != nil || closeErr != nil || ctx.Err() != nil ||
		time.Unix(0, row.ObservedAtUnixNano).Before(startedAt) {
		return nil
	}
	after, err := reader.engine.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil || !matches(after.Container, target, id, replica) ||
		before.Container.RestartCount != after.Container.RestartCount ||
		before.Container.State.StartedAt != after.Container.State.StartedAt {
		return nil
	}
	addContainerDetails(row, before.Container, replica, request.IncludeStorage)
	row.StartedAtUnixNano = startedAt.UnixNano()
	if request.IncludeStorage {
		for _, volume := range request.Volumes {
			for _, mount := range after.Container.Mounts {
				if string(mount.Type) == "volume" && mount.Name == volume.DockerName {
					row.VolumeIds = append(row.VolumeIds, volume.VolumeId)
					break
				}
			}
		}
	}
	return row
}

func matches(
	value container.InspectResponse,
	target *agentpb.ServiceObservationTarget,
	id string,
	replica uint64,
) bool {
	if value.ID != id || value.Config == nil || value.State == nil || value.State.Status != "running" ||
		value.RestartCount < 0 {
		return false
	}
	actual, valid := serviceobservation.WorkloadReplica(value.Config.Labels, target)
	return valid && actual == replica
}
