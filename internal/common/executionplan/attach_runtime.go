package executionplan

import (
	"crypto/sha256"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func attachRuntimeTarget(service *agentpb.ComposeService) string {
	if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
		return service.GetSlot()
	}
	if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
		return "singleton"
	}
	if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA {
		return "rolling"
	}
	return ""
}

func projectAttachRuntimeArtifact(
	source *agentpb.ComposeArtifact,
	selected []*agentpb.ComposeService,
	previous *agentpb.ComposeArtifact,
	previousProxy *agentpb.ComposeService,
	releaseID, target string,
) (*agentpb.ComposeArtifact, error) {
	if err := validateRuntimeWorkloadReplacement(previous, selected, releaseID, target); err != nil {
		return nil, err
	}
	hybrid := proto.CloneOf(previous)
	hybrid.ArtifactId = source.GetArtifactId()
	hybrid.Services = nil
	names := make([]string, len(selected))
	for index, workload := range selected {
		hybrid.Services = append(hybrid.Services, proto.CloneOf(workload))
		names[index] = workload.GetComposeName()
	}
	if previousProxy != nil {
		hybrid.Services = append(hybrid.Services, proto.CloneOf(previousProxy))
	}
	canonical, references, err := attachRuntimeYAML(
		source,
		names,
		previous,
	)
	if err != nil {
		return nil, err
	}
	hybrid.CanonicalYaml = canonical
	digest := sha256.Sum256(canonical)
	hybrid.YamlSha256 = slices.Clone(digest[:])
	hybrid.Networks, err = attachRuntimeNetworks(previous.GetNetworks(), source.GetNetworks(), references["networks"])
	if err != nil {
		return nil, err
	}
	hybrid.Volumes, err = attachRuntimeVolumes(previous.GetVolumes(), source.GetVolumes(), references["volumes"])
	if err != nil {
		return nil, err
	}
	projected := proto.CloneOf(hybrid)
	if err := sealCandidateRuntimeProjection(hybrid, projected, previousProxy, previousProxy.GetProxyConfigJson()); err != nil {
		return nil, err
	}
	return projected, nil
}

func attachRuntimeYAML(
	source *agentpb.ComposeArtifact,
	selectedNames []string,
	previous *agentpb.ComposeArtifact,
) ([]byte, map[string]map[string]bool, error) {
	var sourceDocument, previousDocument yaml.Node
	if yaml.Unmarshal(source.GetCanonicalYaml(), &sourceDocument) != nil ||
		yaml.Unmarshal(previous.GetCanonicalYaml(), &previousDocument) != nil ||
		len(sourceDocument.Content) != 1 || len(previousDocument.Content) != 1 {
		return nil, nil, invalidAttachRuntime("Compose YAML is invalid")
	}
	sourceRoot, previousRoot := sourceDocument.Content[0], previousDocument.Content[0]
	previousServices := candidateRuntimeMappingValue(previousRoot, "services")
	replacements := make(map[int]*yaml.Node, len(selectedNames))
	for _, name := range selectedNames {
		sourceService, err := attachRuntimeYAMLService(sourceRoot, name)
		if err != nil {
			return nil, nil, err
		}
		previousIndex, err := attachRuntimeYAMLMappingIndex(previousServices, name)
		if err != nil || previousIndex < 0 || replacements[previousIndex] != nil {
			return nil, nil, invalidAttachRuntime("predecessor workload YAML is absent or ambiguous")
		}
		replacements[previousIndex] = sourceService
	}
	preserved := make(map[string]map[string]bool, 4)
	for offset := 0; offset+1 < len(previousServices.Content); offset += 2 {
		if replacements[offset] != nil {
			continue
		}
		if err := candidateRuntimeResourceReferences(previousServices.Content[offset+1], preserved); err != nil {
			return nil, nil, err
		}
	}
	references := make(map[string]map[string]bool, 4)
	for offset := 0; offset+1 < len(previousServices.Content); offset += 2 {
		if replacement := replacements[offset]; replacement != nil {
			previousServices.Content[offset+1] = replacement
			if err := candidateRuntimeResourceReferences(replacement, references); err != nil {
				return nil, nil, err
			}
		}
	}
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		for _, name := range attachRuntimeSortedNames(references[section]) {
			if preserved[section][name] {
				before := candidateRuntimeMappingValue(candidateRuntimeMappingValue(previousRoot, section), name)
				after := candidateRuntimeMappingValue(candidateRuntimeMappingValue(sourceRoot, section), name)
				if !sharedRuntimeResourceEqual(section, before, after) ||
					!attachRuntimeSharedMetadataEqual(previous, source, section, name) {
					return nil, nil, invalidAttachRuntime("selected workload changed a preserved resource")
				}
				continue
			}
			if err := attachRuntimeReplaceYAMLResource(previousRoot, sourceRoot, section, name); err != nil {
				return nil, nil, err
			}
		}
	}
	encoded, err := yaml.Marshal(&previousDocument)
	if err != nil {
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, references, nil
}

// YAML mapping order is not resource identity. Sequence order and scalar types
// remain significant; only a semantically unchanged shared resource is safe.
func attachRuntimeYAMLEqual(left, right *yaml.Node) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Kind != right.Kind || left.Tag != right.Tag || left.Value != right.Value ||
		len(left.Content) != len(right.Content) {
		return false
	}
	if left.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(left.Content); index += 2 {
			key := left.Content[index]
			other, err := attachRuntimeYAMLMappingIndex(right, key.Value)
			if key.Kind != yaml.ScalarNode || err != nil || other < 0 ||
				!attachRuntimeYAMLEqual(key, right.Content[other]) ||
				!attachRuntimeYAMLEqual(left.Content[index+1], right.Content[other+1]) {
				return false
			}
		}
		return len(left.Content)%2 == 0
	}
	for index := range left.Content {
		if !attachRuntimeYAMLEqual(left.Content[index], right.Content[index]) {
			return false
		}
	}
	return left.Kind != yaml.AliasNode
}

func attachRuntimeSharedMetadataEqual(previous, source *agentpb.ComposeArtifact, section, name string) bool {
	switch section {
	case "networks":
		var before, after *agentpb.ComposeNetwork
		for _, value := range previous.GetNetworks() {
			if value.GetComposeName() == name {
				before = value
			}
		}
		for _, value := range source.GetNetworks() {
			if value.GetComposeName() == name {
				after = value
			}
		}
		return proto.Equal(before, after)
	case "volumes":
		var before, after *agentpb.ComposeVolume
		for _, value := range previous.GetVolumes() {
			if value.GetComposeName() == name {
				before = value
			}
		}
		for _, value := range source.GetVolumes() {
			if value.GetComposeName() == name {
				after = value
			}
		}
		return proto.Equal(before, after)
	default:
		return true
	}
}

func attachRuntimeYAMLService(root *yaml.Node, name string) (*yaml.Node, error) {
	services := candidateRuntimeMappingValue(root, "services")
	index, err := attachRuntimeYAMLMappingIndex(services, name)
	if err != nil || index < 0 || services.Content[index+1].Kind != yaml.MappingNode {
		return nil, invalidAttachRuntime("selected workload YAML is absent or ambiguous")
	}
	return services.Content[index+1], nil
}

func attachRuntimeYAMLMappingIndex(mapping *yaml.Node, name string) (int, error) {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return -1, invalidAttachRuntime("Compose YAML mapping is invalid")
	}
	index := -1
	for offset := 0; offset+1 < len(mapping.Content); offset += 2 {
		if mapping.Content[offset].Kind != yaml.ScalarNode || mapping.Content[offset].Value != name {
			continue
		}
		if index >= 0 {
			return -1, invalidAttachRuntime("Compose YAML mapping is ambiguous")
		}
		index = offset
	}
	return index, nil
}

func attachRuntimeReplaceYAMLResource(
	destination, source *yaml.Node,
	section, name string,
) error {
	sourceSection := candidateRuntimeMappingValue(source, section)
	sourceIndex, err := attachRuntimeYAMLMappingIndex(sourceSection, name)
	if err != nil && sourceSection != nil {
		return err
	}
	destinationSection := candidateRuntimeMappingValue(destination, section)
	if destinationSection == nil {
		if sourceIndex < 0 {
			return nil
		}
		destination.Content = append(destination.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: section},
			&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"},
		)
		destinationSection = destination.Content[len(destination.Content)-1]
	}
	destinationIndex, destinationErr := attachRuntimeYAMLMappingIndex(destinationSection, name)
	if destinationErr != nil {
		return destinationErr
	}
	if destinationIndex >= 0 {
		destinationSection.Content = append(
			destinationSection.Content[:destinationIndex],
			destinationSection.Content[destinationIndex+2:]...,
		)
	}
	if sourceIndex >= 0 {
		destinationSection.Content = append(
			destinationSection.Content,
			sourceSection.Content[sourceIndex],
			sourceSection.Content[sourceIndex+1],
		)
	}
	return nil
}

func attachRuntimeNetworks(
	previous, source []*agentpb.ComposeNetwork,
	references map[string]bool,
) ([]*agentpb.ComposeNetwork, error) {
	result := make([]*agentpb.ComposeNetwork, 0, len(previous)+len(source))
	for _, value := range previous {
		if !references[value.GetComposeName()] {
			result = append(result, proto.CloneOf(value))
		}
	}
	for _, name := range attachRuntimeSortedNames(references) {
		var selected *agentpb.ComposeNetwork
		for _, value := range source {
			if value.GetComposeName() != name {
				continue
			}
			if selected != nil {
				return nil, invalidAttachRuntime("selected network resource is ambiguous")
			}
			selected = value
		}
		if selected != nil {
			result = append(result, proto.CloneOf(selected))
		}
	}
	slices.SortFunc(result, func(left, right *agentpb.ComposeNetwork) int {
		return strings.Compare(left.GetNetworkId(), right.GetNetworkId())
	})
	return result, nil
}

func attachRuntimeVolumes(
	previous, source []*agentpb.ComposeVolume,
	references map[string]bool,
) ([]*agentpb.ComposeVolume, error) {
	result := make([]*agentpb.ComposeVolume, 0, len(previous)+len(source))
	for _, value := range previous {
		if !references[value.GetComposeName()] {
			result = append(result, proto.CloneOf(value))
		}
	}
	for _, name := range attachRuntimeSortedNames(references) {
		var selected *agentpb.ComposeVolume
		for _, value := range source {
			if value.GetComposeName() != name {
				continue
			}
			if selected != nil {
				return nil, invalidAttachRuntime("selected volume resource is ambiguous")
			}
			selected = value
		}
		if selected != nil {
			result = append(result, proto.CloneOf(selected))
		}
	}
	slices.SortFunc(result, func(left, right *agentpb.ComposeVolume) int {
		return strings.Compare(left.GetVolumeId(), right.GetVolumeId())
	})
	return result, nil
}

func invalidAttachRuntime(detail string) error {
	return errs.New(errs.KindValidationFailed, "Attach runtime "+detail)
}

func attachRuntimeSortedNames(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}
