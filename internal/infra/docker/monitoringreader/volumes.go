package monitoringreader

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
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
	// Storage has a separate budget so a large tree cannot consume the entire
	// workload sampling deadline.
	deadline := time.Now().Add(2 * time.Second)
	if parent, ok := ctx.Deadline(); ok {
		parent = parent.Add(-200 * time.Millisecond)
		if parent.Before(deadline) {
			deadline = parent
		}
	}
	storage, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for index, target := range request.Volumes {
		if storage.Err() != nil {
			break
		}
		if value, ok := reader.volumeSize(storage, target); ok {
			rows[index].UsedBytes = &value
			rows[index].ObservedAtUnixNano = reader.now().UnixNano()
		}
	}
	return rows
}

func (reader *Reader) volumeSize(ctx context.Context, target *agentpb.MonitoringVolumeTarget) (uint64, bool) {
	if target.DockerName != "gp_vol_"+strings.ToLower(target.VolumeId) {
		return 0, false
	}
	inspected, err := reader.engine.VolumeInspect(ctx, target.DockerName, client.VolumeInspectOptions{})
	if err != nil {
		return 0, false
	}
	volume := inspected.Volume
	labels, options := volume.Labels, volume.Options
	if volume.Name != target.DockerName || volume.Driver != "local" ||
		labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "volume" ||
		labels["com.groundplane.environment-id"] != target.EnvironmentId ||
		labels["com.docker.compose.project"] != target.ComposeProject ||
		labels["com.docker.compose.volume"] != target.VolumeKey ||
		len(options) != 3 || options["type"] != "none" || options["o"] != "bind" {
		return 0, false
	}
	device := options["device"]
	directory := path.Dir(device)
	scope, err := environmentpath.Parse(reader.volumeRoot, directory)
	if err != nil || device != directory+"/"+target.VolumeKey || scope.EnvironmentID != target.EnvironmentId ||
		scope.ProjectID != labels["com.groundplane.project-id"] || scope.TenantID != labels["com.groundplane.tenant-id"] {
		return 0, false
	}
	tree, err := backupvolumefs.Open(ctx, reader.volumeRoot, directory, target.VolumeKey)
	if err != nil {
		return 0, false
	}
	defer tree.Close()
	used, err := tree.ApparentSize(ctx)
	return used, err == nil
}
