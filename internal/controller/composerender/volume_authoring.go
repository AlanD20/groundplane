package composerender

import (
	"strings"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// AuthoringComposeVolumes removes only the runtime bind declaration produced
// by a direct managed-Volume mutation. Authored Volume options are preserved.
func AuthoringComposeVolumes(
	compose []byte,
	identities []projectionrecord.EnvironmentVolumeIdentity,
	volumeDirectory string,
) ([]byte, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(compose, &document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "Environment authored Compose is invalid")
	}
	root := document.Content[0]
	volumesIndex := MappingIndex(root, "volumes")
	if volumesIndex < 0 {
		return append([]byte(nil), compose...), nil
	}
	volumes := root.Content[volumesIndex+1]
	if volumes.Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "Environment authored Volumes are invalid")
	}
	byKey := make(map[string]projectionrecord.EnvironmentVolumeIdentity, len(identities))
	for _, identity := range identities {
		byKey[identity.Key] = identity
	}
	changed := false
	for index := 0; index+1 < len(volumes.Content); index += 2 {
		key := volumes.Content[index].Value
		identity, owned := byKey[key]
		volume := volumes.Content[index+1]
		if !owned || volume.Kind != yaml.MappingNode {
			continue
		}
		if markerIndex := MappingIndex(volume, composeResourceExtension); markerIndex >= 0 {
			if !generatedDirectVolume(volume, identity.ID, key, volumeDirectory) {
				return nil, errs.New(errs.KindInternal, "Environment generated Volume declaration is inconsistent")
			}
			authored := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			AppendMappingValue(authored, ComposeVolumeSlugExtension, scalarNode(identity.Slug))
			volumes.Content[index+1] = authored
			changed = true
			continue
		}
		slugIndex := MappingIndex(volume, ComposeVolumeSlugExtension)
		if slugIndex >= 0 {
			if volume.Content[slugIndex+1].Value != identity.Slug {
				volume.Content[slugIndex+1] = scalarNode(identity.Slug)
				changed = true
			}
		} else if identity.Slug != key {
			AppendMappingValue(volume, ComposeVolumeSlugExtension, scalarNode(identity.Slug))
			changed = true
		}
	}
	if !changed {
		return append([]byte(nil), compose...), nil
	}
	authored, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return authored, nil
}

func generatedDirectVolume(volume *yaml.Node, id, key, volumeDirectory string) bool {
	if volumeDirectory == "" ||
		!volumeScalarEquals(volume, "name", "gp_vol_"+strings.ToLower(id)) ||
		!volumeScalarEquals(volume, "driver", "local") {
		return false
	}
	optionsIndex := MappingIndex(volume, "driver_opts")
	markerIndex := MappingIndex(volume, composeResourceExtension)
	if optionsIndex < 0 || markerIndex < 0 {
		return false
	}
	options := volume.Content[optionsIndex+1]
	marker := volume.Content[markerIndex+1]
	if !volumeScalarEquals(options, "type", "none") ||
		!volumeScalarEquals(options, "o", "bind") ||
		!volumeScalarEquals(options, "device", volumeDirectory+"/"+key) ||
		!volumeScalarEquals(marker, "kind", "volume") ||
		!volumeScalarEquals(marker, "id", id) {
		return false
	}
	for index := 0; index+1 < len(volume.Content); index += 2 {
		switch volume.Content[index].Value {
		case "name", "driver", "driver_opts", "labels", composeResourceExtension:
		default:
			return false
		}
	}
	return true
}

func volumeScalarEquals(mapping *yaml.Node, key, value string) bool {
	index := MappingIndex(mapping, key)
	return index >= 0 && mapping.Content[index+1].Kind == yaml.ScalarNode &&
		mapping.Content[index+1].Value == value
}
