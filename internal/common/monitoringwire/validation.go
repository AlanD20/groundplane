// Package monitoringwire bounds the read-only runtime measurement exchange.
package monitoringwire

import (
	"math"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	Timeout             = 8 * time.Second
	MaximumTargets      = 200
	MaximumContainers   = 512
	MaximumVolumes      = 512
	MaximumRequestBytes = 256 << 10
	MaximumResultBytes  = 1 << 20
)

func ValidateRequest(request *agentpb.CollectMonitoringSamples) error {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || ids.Validate(ids.KindOperation, "op_"+request.RequestId) != nil ||
		len(request.Targets) == 0 || len(request.Targets) > MaximumTargets ||
		len(request.Volumes) > MaximumVolumes ||
		proto.Size(request) > MaximumRequestBytes ||
		!request.IncludeStorage && len(request.Volumes) != 0 {
		return invalid()
	}
	services, names := make(map[string]bool), make(map[string]bool)
	for _, target := range request.Targets {
		if serviceobservation.ValidateTarget(target) != nil || target.DatabaseProbe != nil ||
			target.EnvironmentId != request.Targets[0].EnvironmentId || services[target.ServiceId] {
			return invalid()
		}
		services[target.ServiceId] = true
		for _, workload := range target.Workloads {
			if names[workload.ComposeName] {
				return invalid()
			}
			names[workload.ComposeName] = true
		}
	}
	volumes, dockerNames := make(map[string]bool), make(map[string]bool)
	for _, volume := range request.Volumes {
		if volume == nil || len(volume.ProtoReflect().GetUnknown()) != 0 ||
			ids.Validate(
				ids.KindVolume,
				volume.VolumeId,
			) != nil || volume.EnvironmentId != request.Targets[0].EnvironmentId ||
			!nameValid(volume.DockerName) || !nameValid(volume.ComposeProject) || !nameValid(volume.VolumeKey) ||
			volumes[volume.VolumeId] || dockerNames[volume.DockerName] {
			return invalid()
		}
		volumes[volume.VolumeId], dockerNames[volume.DockerName] = true, true
	}
	return nil
}

func ValidateCancel(request *agentpb.CancelMonitoringSamples) error {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		ids.Validate(ids.KindOperation, "op_"+request.RequestId) != nil {
		return invalid()
	}
	return nil
}

func ValidateResult(request *agentpb.CollectMonitoringSamples, result *agentpb.MonitoringSamplesResult) error {
	if ValidateRequest(request) != nil || result == nil || len(result.ProtoReflect().GetUnknown()) != 0 ||
		result.RequestId != request.RequestId || len(result.Services) != len(request.Targets) ||
		len(result.Volumes) != len(request.Volumes) || proto.Size(result) > MaximumResultBytes {
		return invalid()
	}
	volumes := make(map[string]bool, len(request.Volumes))
	for index, target := range request.Volumes {
		row := result.Volumes[index]
		if row == nil || len(row.ProtoReflect().GetUnknown()) != 0 || row.VolumeId != target.VolumeId ||
			row.ObservedAtUnixNano <= 0 {
			return invalid()
		}
		volumes[target.VolumeId] = true
	}
	seen := make(map[string]bool)
	for index, target := range request.Targets {
		row := result.Services[index]
		if row == nil || len(row.ProtoReflect().GetUnknown()) != 0 || row.ServiceId != target.ServiceId ||
			row.ReleaseId != target.ReleaseId || row.Unavailable != (len(row.Containers) == 0) {
			return invalid()
		}
		replicas := make(map[uint32]bool)
		for _, sample := range row.Containers {
			if !containerValid(sample, request.IncludeStorage, volumes) || seen[sample.ContainerId] ||
				replicas[sample.Replica] {
				return invalid()
			}
			seen[sample.ContainerId], replicas[sample.Replica] = true, true
			if len(seen) > MaximumContainers {
				return invalid()
			}
		}
	}
	return nil
}

func containerValid(sample *agentpb.MonitoringContainerSample, storage bool, volumes map[string]bool) bool {
	if sample == nil || len(sample.ProtoReflect().GetUnknown()) != 0 || len(sample.ContainerId) != 64 ||
		sample.Replica == 0 || sample.Replica > 4096 || sample.ObservedAtUnixNano <= 0 ||
		!storage && (sample.WritableLayerBytes != nil || len(sample.VolumeIds) > 0) {
		return false
	}
	for _, char := range sample.ContainerId {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	if sample.CpuLimitCores != nil &&
		(math.IsNaN(*sample.CpuLimitCores) || math.IsInf(*sample.CpuLimitCores, 0) || *sample.CpuLimitCores <= 0) {
		return false
	}
	if sample.MemoryInactiveFileBytes != nil &&
		(sample.MemoryUsageBytes == nil || *sample.MemoryInactiveFileBytes > *sample.MemoryUsageBytes) ||
		sample.MemoryLimitBytes != nil && *sample.MemoryLimitBytes == 0 ||
		(sample.NetworkReceiveBytes == nil) != (sample.NetworkTransmitBytes == nil) {
		return false
	}
	seen := make(map[string]bool)
	for _, id := range sample.VolumeIds {
		if !volumes[id] || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func nameValid(value string) bool {
	if value == "" || len(value) > 255 || strings.Contains(value, "..") {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
			return false
		}
	}
	return true
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "invalid monitoring measurement exchange")
}
