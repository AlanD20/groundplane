package controller

import (
	"crypto/sha256"
	"sort"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type VolumeArtifactAction string

const (
	VolumeArtifactAdd    VolumeArtifactAction = "add"
	VolumeArtifactEdit   VolumeArtifactAction = "edit"
	VolumeArtifactRemove VolumeArtifactAction = "remove"
)

type VolumeArtifactMutation struct {
	Action           VolumeArtifactAction
	VolumeID         string
	Key              string
	ArtifactID       string
	PlanID           string
	TenantID         string
	ProjectID        string
	RenderGeneration uint64
}

// MutateEnvironmentVolumeArtifact derives one complete immutable runtime
// artifact from the previously published normalized artifact. It changes only
// Controller-owned runtime fields; operator Blueprint bytes are never reparsed
// or treated as mutation authority.
func MutateEnvironmentVolumeArtifact(
	current *agentpb.ComposeArtifact,
	mutation VolumeArtifactMutation,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || current.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindEnvironment, current.GetOwnerId()) != nil ||
		ids.Validate(ids.KindVolume, mutation.VolumeID) != nil ||
		ids.Validate(ids.KindConfig, mutation.ArtifactID) != nil || ids.Validate(ids.KindPlan, mutation.PlanID) != nil ||
		ids.Validate(ids.KindTenant, mutation.TenantID) != nil || ids.Validate(ids.KindProject, mutation.ProjectID) != nil ||
		mutation.RenderGeneration == 0 ||
		(mutation.Action != VolumeArtifactAdd && mutation.Action != VolumeArtifactEdit && mutation.Action != VolumeArtifactRemove) ||
		!validVolumeArtifactKey(mutation.Key) {
		return nil, errs.New(errs.KindInternal, "Volume artifact mutation input is invalid")
	}
	owned := proto.Clone(current).(*agentpb.ComposeArtifact)
	var document yaml.Node
	if err := yaml.Unmarshal(owned.GetCanonicalYaml(), &document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "normalized Compose artifact YAML is corrupt")
	}
	root := document.Content[0]
	volumes, err := volumeArtifactMapping(root, mutation.Action == VolumeArtifactAdd)
	if err != nil {
		return nil, err
	}
	found := mappingIndex(volumes, mutation.Key)
	switch mutation.Action {
	case VolumeArtifactAdd:
		if found >= 0 || composeArtifactHasVolume(owned, mutation.VolumeID, mutation.Key) {
			return nil, errs.New(errs.KindStateConflict, "Volume artifact identity is already present")
		}
		labels := volumeArtifactOwnershipLabels(current.GetOwnerId(), mutation)
		appendMappingValue(volumes, mutation.Key, renderedVolumeNode(
			current.GetAuthorizedVolumeDir(), current.GetOwnerId(), mutation, labels,
		))
		sortMapping(volumes)
		owned.Volumes = append(owned.Volumes, &agentpb.ComposeVolume{
			VolumeId: mutation.VolumeID, ComposeName: mutation.Key,
			DockerName: "gp_vol_" + strings.ToLower(mutation.VolumeID), ExpectedLabels: labelPairs(labels),
		})
	case VolumeArtifactEdit:
		if found < 0 || !composeArtifactHasVolume(owned, mutation.VolumeID, mutation.Key) {
			return nil, errs.New(errs.KindStateConflict, "Volume artifact identity is absent")
		}
	case VolumeArtifactRemove:
		if found < 0 || !composeArtifactHasVolume(owned, mutation.VolumeID, mutation.Key) {
			return nil, errs.New(errs.KindStateConflict, "Volume artifact identity is absent")
		}
		if err := removeVolumeArtifactMounts(root, mutation.Key); err != nil {
			return nil, err
		}
		volumes.Content = append(volumes.Content[:found], volumes.Content[found+2:]...)
		kept := owned.Volumes[:0]
		for _, volume := range owned.Volumes {
			if volume.GetVolumeId() != mutation.VolumeID {
				kept = append(kept, volume)
			}
		}
		owned.Volumes = kept
	}
	if err := rewriteArtifactOwnership(root, mutation.PlanID, mutation.RenderGeneration); err != nil {
		return nil, err
	}
	rewriteLabelPairs := func(values []*agentpb.LabelPair) {
		for _, pair := range values {
			if pair == nil {
				continue
			}
			switch pair.Key {
			case composeLabelPlanID:
				pair.Value = mutation.PlanID
			case composeLabelRenderGen:
				pair.Value = strconv.FormatUint(mutation.RenderGeneration, 10)
			}
		}
	}
	for _, service := range owned.Services {
		rewriteLabelPairs(service.GetExpectedLabels())
	}
	for _, network := range owned.Networks {
		rewriteLabelPairs(network.GetExpectedLabels())
	}
	for _, volume := range owned.Volumes {
		rewriteLabelPairs(volume.GetExpectedLabels())
	}
	sort.Slice(owned.Volumes, func(left, right int) bool {
		return owned.Volumes[left].GetVolumeId() < owned.Volumes[right].GetVolumeId()
	})
	canonical, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(canonical)
	owned.ArtifactId = mutation.ArtifactID
	owned.CanonicalYaml = canonical
	owned.YamlSha256 = digest[:]
	return owned, nil
}

func removeVolumeArtifactMounts(root *yaml.Node, key string) error {
	servicesIndex := mappingIndex(root, "services")
	if servicesIndex < 0 {
		return nil
	}
	services := root.Content[servicesIndex+1]
	if services.Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "normalized Compose Service mapping is corrupt")
	}
	for index := 1; index < len(services.Content); index += 2 {
		service := services.Content[index]
		mountsIndex := mappingIndex(service, "volumes")
		if mountsIndex < 0 {
			continue
		}
		mounts := service.Content[mountsIndex+1]
		if mounts.Kind != yaml.SequenceNode {
			return errs.New(errs.KindInternal, "normalized Compose Volume mounts are corrupt")
		}
		kept := mounts.Content[:0]
		for _, mount := range mounts.Content {
			usesVolume, err := volumeArtifactMountUsesKey(mount, key)
			if err != nil {
				return err
			}
			if !usesVolume {
				kept = append(kept, mount)
			}
		}
		mounts.Content = kept
	}
	return nil
}

func volumeArtifactMountUsesKey(mount *yaml.Node, key string) (bool, error) {
	switch mount.Kind {
	case yaml.ScalarNode:
		source, _, _ := strings.Cut(mount.Value, ":")
		return source == key, nil
	case yaml.MappingNode:
		sourceIndex := mappingIndex(mount, "source")
		if sourceIndex < 0 || mount.Content[sourceIndex+1].Kind != yaml.ScalarNode {
			return false, errs.New(errs.KindInternal, "normalized Compose Volume mount source is corrupt")
		}
		typeIndex := mappingIndex(mount, "type")
		if typeIndex >= 0 && (mount.Content[typeIndex+1].Kind != yaml.ScalarNode ||
			mount.Content[typeIndex+1].Value != "volume") {
			return false, nil
		}
		return mount.Content[sourceIndex+1].Value == key, nil
	default:
		return false, errs.New(errs.KindInternal, "normalized Compose Volume mount is corrupt")
	}
}

func validVolumeArtifactKey(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 255 {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func volumeArtifactMapping(root *yaml.Node, create bool) (*yaml.Node, error) {
	index := mappingIndex(root, "volumes")
	if index >= 0 {
		value := root.Content[index+1]
		if value.Kind != yaml.MappingNode {
			return nil, errs.New(errs.KindInternal, "normalized Compose Volume mapping is corrupt")
		}
		return value, nil
	}
	if !create {
		return nil, errs.New(errs.KindStateConflict, "normalized Compose Volume mapping is absent")
	}
	value := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(root, "volumes", value)
	return value, nil
}

func mappingIndex(mapping *yaml.Node, key string) int {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return -1
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return index
		}
	}
	return -1
}

func appendMappingValue(mapping *yaml.Node, key string, value *yaml.Node) {
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, value,
	)
}

func sortMapping(mapping *yaml.Node) {
	type pair struct{ key, value *yaml.Node }
	pairs := make([]pair, 0, len(mapping.Content)/2)
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		pairs = append(pairs, pair{key: mapping.Content[index], value: mapping.Content[index+1]})
	}
	sort.Slice(pairs, func(left, right int) bool { return pairs[left].key.Value < pairs[right].key.Value })
	mapping.Content = mapping.Content[:0]
	for _, pair := range pairs {
		mapping.Content = append(mapping.Content, pair.key, pair.value)
	}
}

func renderedVolumeNode(
	volumeDirectory string,
	environmentID string,
	mutation VolumeArtifactMutation,
	labels map[string]string,
) *yaml.Node {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(node, "name", scalarNode("gp_vol_"+strings.ToLower(mutation.VolumeID)))
	appendMappingValue(node, "driver", scalarNode("local"))
	driverOptions := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(driverOptions, "type", scalarNode("none"))
	appendMappingValue(driverOptions, "o", scalarNode("bind"))
	appendMappingValue(driverOptions, "device", scalarNode(volumeDirectory+"/"+mutation.Key))
	appendMappingValue(node, "driver_opts", driverOptions)
	labelNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		appendMappingValue(labelNode, key, scalarNode(labels[key]))
	}
	appendMappingValue(node, "labels", labelNode)
	resource := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(resource, "kind", scalarNode("volume"))
	appendMappingValue(resource, "id", scalarNode(mutation.VolumeID))
	parent := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendMappingValue(parent, "environment_id", scalarNode(environmentID))
	appendMappingValue(resource, "parent", parent)
	appendMappingValue(node, composeResourceExtension, resource)
	return node
}

func scalarNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

func volumeArtifactOwnershipLabels(environmentID string, mutation VolumeArtifactMutation) map[string]string {
	return map[string]string{
		composeLabelEnvironmentID: environmentID,
		composeLabelKind:          "volume", composeLabelManaged: "true", composeLabelPlanID: mutation.PlanID,
		composeLabelProjectID: mutation.ProjectID,
		composeLabelRenderGen: strconv.FormatUint(mutation.RenderGeneration, 10),
		composeLabelTenantID:  mutation.TenantID,
	}
}

func labelPairs(labels map[string]string) []*agentpb.LabelPair {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*agentpb.LabelPair, 0, len(keys))
	for _, key := range keys {
		result = append(result, &agentpb.LabelPair{Key: key, Value: labels[key]})
	}
	return result
}

func composeArtifactHasVolume(artifact *agentpb.ComposeArtifact, volumeID string, key string) bool {
	for _, volume := range artifact.GetVolumes() {
		if volume.GetVolumeId() == volumeID || volume.GetComposeName() == key {
			return volume.GetVolumeId() == volumeID && volume.GetComposeName() == key &&
				volume.GetDockerName() == "gp_vol_"+strings.ToLower(volumeID)
		}
	}
	return false
}

func rewriteArtifactOwnership(root *yaml.Node, planID string, generation uint64) error {
	for _, section := range []string{"services", "networks", "volumes"} {
		sectionIndex := mappingIndex(root, section)
		if sectionIndex < 0 {
			continue
		}
		resources := root.Content[sectionIndex+1]
		if resources.Kind != yaml.MappingNode {
			return errs.New(errs.KindInternal, "normalized Compose resource mapping is corrupt")
		}
		for index := 1; index < len(resources.Content); index += 2 {
			resource := resources.Content[index]
			labelsIndex := mappingIndex(resource, "labels")
			if labelsIndex < 0 {
				continue
			}
			labels := resource.Content[labelsIndex+1]
			if labels.Kind != yaml.MappingNode {
				return errs.New(errs.KindInternal, "normalized Compose ownership labels are corrupt")
			}
			setMappingScalar(labels, composeLabelPlanID, planID)
			setMappingScalar(labels, composeLabelRenderGen, strconv.FormatUint(generation, 10))
		}
	}
	return nil
}

func setMappingScalar(mapping *yaml.Node, key string, value string) {
	index := mappingIndex(mapping, key)
	if index >= 0 {
		mapping.Content[index+1] = scalarNode(value)
		return
	}
	appendMappingValue(mapping, key, scalarNode(value))
	sortMapping(mapping)
}
