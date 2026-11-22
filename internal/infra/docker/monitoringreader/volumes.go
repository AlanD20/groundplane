package monitoringreader

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/client"
)

func (reader *Reader) sampleVolumes(
	ctx context.Context,
	request *agentpb.CollectMonitoringSamples,
) []*agentpb.MonitoringVolumeSample {
	rows := make([]*agentpb.MonitoringVolumeSample, len(request.Volumes))
	for index, target := range request.Volumes {
		rows[index] = &agentpb.MonitoringVolumeSample{
			VolumeId:           target.VolumeId,
			ObservedAtUnixNano: reader.now().UnixNano(),
		}
	}
	if len(rows) == 0 {
		return rows
	}
	usage, err := reader.engine.DiskUsage(ctx, client.DiskUsageOptions{Volumes: true, Verbose: true})
	if err != nil || len(usage.Volumes.Items) > 4096 {
		return rows
	}
	for index, target := range request.Volumes {
		var found bool
		for _, volume := range usage.Volumes.Items {
			if volume.Name != target.DockerName {
				continue
			}
			if found {
				rows[index].UsedBytes = nil
				break
			}
			found = true
			labels := volume.Labels
			if volume.Driver != "local" || labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "volume" ||
				labels["com.groundplane.environment-id"] != target.EnvironmentId || labels["com.docker.compose.project"] != target.ComposeProject ||
				labels["com.docker.compose.volume"] != target.VolumeKey ||
				volume.UsageData == nil ||
				volume.UsageData.Size < 0 {
				continue
			}
			value := uint64(volume.UsageData.Size)
			rows[index].UsedBytes = &value
		}
	}
	return rows
}
