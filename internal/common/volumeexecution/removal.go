// Package volumeexecution owns captured Volume operations independently of the
// Task scheduler and generic execution-step representation.
package volumeexecution

import (
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Removal selects only the captured workload instances that mounted the
// Volume. Logical Service IDs alone cannot select a blue-green slot or exclude
// its proxy. Artifact integrity and host-path authorization remain required at
// the delivery and host boundaries; this value does not grant data deletion.
type Removal struct {
	VolumeID    string
	Key         string
	Baseline    *agentpb.ComposeArtifact
	Candidate   *agentpb.ComposeArtifact
	ConsumerIDs []string
}

func (removal Removal) Consumers() ([]string, error) {
	baseline, candidate := removal.Baseline, removal.Candidate
	if ids.Validate(ids.KindVolume, removal.VolumeID) != nil || volumeidentity.ValidateKey(removal.Key) != nil ||
		baseline == nil || candidate == nil || baseline == candidate ||
		baseline.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, baseline.OwnerId) != nil || candidate.OwnerKind != baseline.OwnerKind ||
		candidate.OwnerId != baseline.OwnerId || candidate.ProjectName != baseline.ProjectName ||
		candidate.AuthorizedVolumeDir != baseline.AuthorizedVolumeDir || len(candidate.Services) != len(baseline.Services) {
		return nil, invalidRemoval()
	}
	found := false
	for _, volume := range baseline.Volumes {
		if volume.GetVolumeId() == removal.VolumeID {
			if found || volume.GetComposeName() != removal.Key || volume.GetDockerName() != "gp_vol_"+strings.ToLower(removal.VolumeID) {
				return nil, invalidRemoval()
			}
			found = true
		}
	}
	for _, volume := range candidate.Volumes {
		if volume.GetVolumeId() == removal.VolumeID || volume.GetComposeName() == removal.Key {
			return nil, invalidRemoval()
		}
	}
	if !found {
		return nil, invalidRemoval()
	}
	selected := make(map[string]struct{}, len(removal.ConsumerIDs))
	for _, id := range removal.ConsumerIDs {
		if ids.Validate(ids.KindService, id) != nil {
			return nil, invalidRemoval()
		}
		if _, duplicate := selected[id]; duplicate {
			return nil, invalidRemoval()
		}
		selected[id] = struct{}{}
	}
	prior, err := mountedServices(baseline, removal.Key)
	if err != nil {
		return nil, err
	}
	next, err := mountedServices(candidate, removal.Key)
	if err != nil {
		return nil, err
	}
	var names []string
	for index, service := range baseline.Services {
		if service == nil || !proto.Equal(service, candidate.Services[index]) || next[service.ComposeName] {
			return nil, invalidRemoval()
		}
		if prior[service.ComposeName] {
			if _, exists := selected[service.ServiceId]; !exists {
				return nil, invalidRemoval()
			}
			names = append(names, service.ComposeName)
		}
	}
	// Desired consumers may not have a deployed instance yet. Only mounts in
	// the captured runtime select Compose names; do not start an absent consumer.
	sort.Strings(names)
	return names, nil
}

func mountedServices(artifact *agentpb.ComposeArtifact, key string) (map[string]bool, error) {
	var document struct {
		Services map[string]struct {
			Volumes []yaml.Node `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	if len(document.Services) != len(artifact.Services) {
		return nil, invalidRemoval()
	}
	result := make(map[string]bool, len(document.Services))
	for _, service := range artifact.Services {
		entry, ok := document.Services[service.GetComposeName()]
		if !ok || service == nil {
			return nil, invalidRemoval()
		}
		if _, duplicate := result[service.ComposeName]; duplicate {
			return nil, invalidRemoval()
		}
		result[service.ComposeName] = false
		for _, mount := range entry.Volumes {
			var source, kind string
			switch mount.Kind {
			case yaml.ScalarNode:
				source, _, _ = strings.Cut(mount.Value, ":")
			case yaml.MappingNode:
				var value struct {
					Source string `yaml:"source"`
					Type   string `yaml:"type"`
				}
				if err := mount.Decode(&value); err != nil {
					return nil, errs.Wrap(errs.KindValidationFailed, err)
				}
				source, kind = value.Source, value.Type
			default:
				return nil, invalidRemoval()
			}
			if source == key && (kind == "" || kind == "volume") {
				result[service.ComposeName] = true
			}
		}
	}
	return result, nil
}

func invalidRemoval() error {
	return errs.New(errs.KindValidationFailed, "Volume removal must capture the exact mounted workloads and their detached configuration")
}
