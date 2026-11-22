package serviceobservation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// MonitoringCapture is an immutable view of exact acknowledged runtime
// authority. Its accessors clone wire values so collectors cannot retarget a
// later recheck by mutating the capture.
type MonitoringCapture struct {
	source      source
	target      *agentpb.ServiceObservationTarget
	volumes     []*agentpb.MonitoringVolumeTarget
	expected    uint32
	fingerprint monitoring.SourceFingerprint
}

func (capture MonitoringCapture) Target() *agentpb.ServiceObservationTarget {
	return proto.CloneOf(capture.target)
}

func (capture MonitoringCapture) Volumes() []*agentpb.MonitoringVolumeTarget {
	result := make([]*agentpb.MonitoringVolumeTarget, len(capture.volumes))
	for index, volume := range capture.volumes {
		result[index] = proto.CloneOf(volume)
	}
	return result
}

func (capture MonitoringCapture) ExpectedReplicas() uint32 { return capture.expected }

func (capture MonitoringCapture) Fingerprint() monitoring.SourceFingerprint {
	return capture.fingerprint
}

// CaptureMonitoring selects only acknowledged workload and named-Volume
// authority. Database probes carry separate secret-bearing preflight authority
// and are never part of continuous collection.
func CaptureMonitoring(
	ctx context.Context,
	releases Releases,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
) (MonitoringCapture, bool) {
	captured, ok := capture(ctx, releases, service)
	if !ok || captured.target == nil || captured.target.DatabaseProbe != nil {
		return MonitoringCapture{}, false
	}
	target := proto.CloneOf(captured.target)
	target.DatabaseProbe = nil
	volumes, ok := monitoringVolumeTargets(service.Record.Desired.ID, captured)
	if !ok {
		return MonitoringCapture{}, false
	}
	fingerprint, ok := monitoringFingerprint(captured, target, volumes)
	if !ok {
		return MonitoringCapture{}, false
	}
	authority := captured
	authority.monitoringArtifact = nil
	authority.monitoringArtifactBytes = nil
	authority.monitoringVolumes = nil
	authority.monitoringMounts = nil
	return MonitoringCapture{
		source: authority, target: target, volumes: volumes,
		expected: captured.expected, fingerprint: fingerprint,
	}, true
}

// RecheckMonitoring requires a fresh fixed-revision Service read and every
// source MVCC fence captured before the Agent exchange. Equal values after an
// intervening write are rejected.
func RecheckMonitoring(
	ctx context.Context,
	releases Releases,
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	captured MonitoringCapture,
) bool {
	if captured.target == nil || captured.fingerprint == (monitoring.SourceFingerprint{}) ||
		current.Record.EnvironmentID != captured.target.EnvironmentId ||
		current.Record.Desired.ID != captured.target.ServiceId {
		return false
	}
	checked, ok := CaptureMonitoring(ctx, releases, current)
	return ok && captured.source.unchanged(checked.source) &&
		captured.fingerprint == checked.fingerprint && proto.Equal(captured.target, checked.target) &&
		monitoringVolumesEqual(captured.volumes, checked.volumes)
}

func monitoringVolumeTargets(serviceID string, captured source) ([]*agentpb.MonitoringVolumeTarget, bool) {
	artifact := captured.monitoringArtifact
	if artifact == nil && len(captured.monitoringArtifactBytes) != 0 {
		artifact = new(agentpb.ComposeArtifact)
		if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
			captured.monitoringArtifactBytes,
			artifact,
		) != nil {
			return nil, false
		}
	}
	if artifact == nil || artifact.OwnerId != captured.target.EnvironmentId ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.ProjectName != "gp-"+strings.ToLower(captured.target.EnvironmentId) {
		return nil, false
	}
	stable := make(map[string]string, len(captured.monitoringVolumes))
	stableKeys := make(map[string]bool, len(captured.monitoringVolumes))
	for _, volume := range captured.monitoringVolumes {
		if ids.Validate(ids.KindVolume, volume.ID) != nil || volumeidentity.ValidateKey(volume.Key) != nil {
			return nil, false
		}
		if _, duplicate := stable[volume.ID]; duplicate || stableKeys[volume.Key] {
			return nil, false
		}
		stable[volume.ID] = volume.Key
		stableKeys[volume.Key] = true
	}
	allowed := make(map[string]bool)
	if captured.monitoringArtifactVolumes {
		for _, volume := range artifact.Volumes {
			allowed[volume.GetVolumeId()] = true
		}
	} else {
		for _, mount := range captured.monitoringMounts {
			if mount.ServiceID == serviceID {
				allowed[mount.VolumeID] = true
			}
		}
	}
	result := make([]*agentpb.MonitoringVolumeTarget, 0, len(allowed))
	seenIDs, seenKeys, seenDocker := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for _, volume := range artifact.Volumes {
		if volume == nil || !allowed[volume.VolumeId] {
			continue
		}
		if seenIDs[volume.VolumeId] || seenKeys[volume.ComposeName] ||
			seenDocker[volume.DockerName] || ids.Validate(ids.KindVolume, volume.VolumeId) != nil ||
			volumeidentity.ValidateKey(volume.ComposeName) != nil ||
			volume.DockerName != "gp_vol_"+strings.ToLower(volume.VolumeId) ||
			!captured.monitoringArtifactVolumes && stable[volume.VolumeId] != volume.ComposeName {
			return nil, false
		}
		seenIDs[volume.VolumeId], seenKeys[volume.ComposeName], seenDocker[volume.DockerName] = true, true, true
		result = append(result, &agentpb.MonitoringVolumeTarget{
			VolumeId: volume.VolumeId, EnvironmentId: captured.target.EnvironmentId,
			DockerName: volume.DockerName, ComposeProject: artifact.ProjectName, VolumeKey: volume.ComposeName,
		})
	}
	if len(result) != len(allowed) {
		return nil, false
	}
	for _, volume := range artifact.Volumes {
		if volume == nil || allowed[volume.VolumeId] {
			continue
		}
		if seenKeys[volume.ComposeName] || seenDocker[volume.DockerName] {
			return nil, false
		}
	}
	slices.SortFunc(result, func(left, right *agentpb.MonitoringVolumeTarget) int {
		return strings.Compare(left.VolumeId, right.VolumeId)
	})
	return result, true
}

func monitoringFingerprint(
	captured source,
	target *agentpb.ServiceObservationTarget,
	volumes []*agentpb.MonitoringVolumeTarget,
) (monitoring.SourceFingerprint, bool) {
	digest := sha256.New()
	_, _ = digest.Write([]byte("groundplane-monitoring-source-v1\x00"))
	writeInt64 := func(value int64) { _ = binary.Write(digest, binary.BigEndian, value) }
	writeInt64(captured.runtimeRevision)
	writeInt64(captured.projectionRevision)
	writeInt64(captured.intentRevision)
	writeInt64(captured.renderRevision)
	writeInt64(captured.acknowledgedRuntimeRevision)
	_ = binary.Write(digest, binary.BigEndian, captured.expected)
	values := make([]proto.Message, 0, 1+len(volumes))
	values = append(values, target)
	for _, volume := range volumes {
		values = append(values, volume)
	}
	for _, value := range values {
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(value)
		if err != nil {
			return monitoring.SourceFingerprint{}, false
		}
		_ = binary.Write(digest, binary.BigEndian, uint32(len(encoded)))
		_, _ = digest.Write(encoded)
		clear(encoded)
	}
	var fingerprint monitoring.SourceFingerprint
	copy(fingerprint[:], digest.Sum(nil))
	return fingerprint, fingerprint != (monitoring.SourceFingerprint{})
}

func monitoringVolumesEqual(left, right []*agentpb.MonitoringVolumeTarget) bool {
	return len(left) == len(right) && slices.EqualFunc(left, right, func(a, b *agentpb.MonitoringVolumeTarget) bool {
		return proto.Equal(a, b)
	})
}
