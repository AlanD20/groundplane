package composerender

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

type ServiceArtifactVolumeMount struct {
	Key      string
	Target   string
	ReadOnly bool
}

// Replace only managed Volume mounts. File binds and native configs/secrets
// remain owned by their existing configuration paths.
func replaceServiceArtifactVolumeMounts(service *yaml.Node, mounts []ServiceArtifactVolumeMount) error {
	retained := make([]*yaml.Node, 0)
	previous := make(map[string]*yaml.Node)
	if index := MappingIndex(service, "volumes"); index >= 0 {
		sequence := service.Content[index+1]
		if sequence.Kind != yaml.SequenceNode {
			return errs.New(errs.KindInternal, "Service mount configuration is corrupt")
		}
		for _, node := range sequence.Content {
			if node.Kind != yaml.MappingNode {
				return errs.New(errs.KindInternal, "Service mount configuration is not normalized")
			}
			typeIndex := MappingIndex(node, "type")
			if typeIndex >= 0 && node.Content[typeIndex+1].Value == "volume" {
				source, target, err := entryArtifactMountIdentity(node)
				if err != nil {
					return err
				}
				previous[source+"\x00"+target] = node
			} else {
				retained = append(retained, node)
			}
		}
	}
	for _, mount := range mounts {
		for _, node := range retained {
			_, target, err := entryArtifactMountIdentity(node)
			if err != nil {
				return err
			}
			if target == mount.Target {
				return errs.New(errs.KindValidationFailed, "Volume mount target conflicts with an existing file mount")
			}
		}
		for _, kind := range []string{"configs", "secrets"} {
			if index := MappingIndex(service, kind); index >= 0 {
				for _, node := range service.Content[index+1].Content {
					if target := MappingIndex(node, "target"); target >= 0 &&
						node.Content[target+1].Value == mount.Target {
						return errs.New(
							errs.KindValidationFailed,
							"Volume mount target conflicts with a config or secret",
						)
					}
				}
			}
		}
		node := previous[mount.Key+"\x00"+mount.Target]
		if node == nil {
			node = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			AppendMappingValue(node, "type", scalarNode("volume"))
			AppendMappingValue(node, "source", scalarNode(mount.Key))
			AppendMappingValue(node, "target", scalarNode(mount.Target))
		}
		if mount.ReadOnly {
			setMappingTypedScalar(node, "read_only", "!!bool", "true")
		} else {
			RemoveMappingValue(node, "read_only")
		}
		retained = append(retained, node)
	}
	if len(retained) == 0 {
		RemoveMappingValue(service, "volumes")
	} else {
		setMappingNode(service, "volumes", &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: retained})
	}
	return nil
}
