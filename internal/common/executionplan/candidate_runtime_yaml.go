package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"gopkg.in/yaml.v3"
	"strings"
)

func projectCandidateRuntimeYAML(
	source *agentpb.ComposeArtifact,
	selected []*agentpb.ComposeService,
	proxy *agentpb.ComposeService,
	sourceProxyConfig []byte,
) ([]byte, map[string]map[string]bool, error) {
	var document yaml.Node
	if yaml.Unmarshal(source.GetCanonicalYaml(), &document) != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose YAML is invalid")
	}
	root := document.Content[0]
	services := candidateRuntimeMappingValue(root, "services")
	if services == nil || services.Kind != yaml.MappingNode {
		return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose Service mapping is absent")
	}
	wanted := make(map[string]bool, len(selected))
	for _, service := range selected {
		if service == nil || service.GetComposeName() == "" || wanted[service.GetComposeName()] {
			return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Service selection is invalid")
		}
		wanted[service.GetComposeName()] = true
	}
	kept := make([]*yaml.Node, 0, len(selected)*2)
	found := make(map[string]bool, len(selected))
	references := make(map[string]map[string]bool, 4)
	var proxyNode *yaml.Node
	for index := 0; index+1 < len(services.Content); index += 2 {
		name, value := services.Content[index], services.Content[index+1]
		if name.Kind != yaml.ScalarNode || !wanted[name.Value] {
			continue
		}
		if found[name.Value] || value.Kind != yaml.MappingNode {
			return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose Service is ambiguous")
		}
		found[name.Value] = true
		kept = append(kept, name, value)
		if err := candidateRuntimeResourceReferences(value, references); err != nil {
			return nil, nil, err
		}
		if proxy != nil && name.Value == proxy.GetComposeName() {
			proxyNode = value
		}
	}
	if len(found) != len(wanted) {
		return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose Service is absent")
	}
	if proxy != nil {
		if proxyNode == nil || candidateRuntimeSetProxyConfig(root, proxyNode, proxy, sourceProxyConfig) != nil {
			return nil, nil, errs.New(
				errs.KindValidationFailed,
				"candidate runtime proxy YAML does not bind its sealed configuration",
			)
		}
	}
	services.Content = kept
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		candidateRuntimePruneResources(root, section, references[section])
	}
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, references, nil
}

func candidateRuntimeResourceReferences(service *yaml.Node, references map[string]map[string]bool) error {
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		entries := candidateRuntimeMappingValue(service, section)
		if entries == nil {
			continue
		}
		if references[section] == nil {
			references[section] = make(map[string]bool)
		}
		if section == "networks" && entries.Kind == yaml.MappingNode {
			for index := 0; index+1 < len(entries.Content); index += 2 {
				if entries.Content[index].Kind != yaml.ScalarNode {
					return errs.New(errs.KindValidationFailed, "candidate runtime network reference is invalid")
				}
				references[section][entries.Content[index].Value] = true
			}
			continue
		}
		if entries.Kind != yaml.SequenceNode {
			return errs.New(errs.KindValidationFailed, "candidate runtime resource references are invalid")
		}
		for _, entry := range entries.Content {
			name, include, err := candidateRuntimeResourceReference(section, entry)
			if err != nil {
				return err
			}
			if include {
				references[section][name] = true
			}
		}
	}
	return nil
}

func candidateRuntimeResourceReference(section string, entry *yaml.Node) (string, bool, error) {
	if entry.Kind == yaml.ScalarNode {
		name := entry.Value
		if section == "volumes" {
			name, _, _ = strings.Cut(name, ":")
		}
		return name, name != "", nil
	}
	if entry.Kind != yaml.MappingNode {
		return "", false, errs.New(errs.KindValidationFailed, "candidate runtime resource reference is invalid")
	}
	if section == "volumes" {
		kind := candidateRuntimeMappingValue(entry, "type")
		if kind != nil && kind.Value != "volume" {
			return "", false, nil
		}
	}
	source := candidateRuntimeMappingValue(entry, "source")
	if source == nil || source.Kind != yaml.ScalarNode || source.Value == "" {
		return "", false, errs.New(errs.KindValidationFailed, "candidate runtime resource source is invalid")
	}
	return source.Value, true, nil
}

func candidateRuntimeSetProxyConfig(
	root, service *yaml.Node,
	proxy *agentpb.ComposeService,
	sourceProxyConfig []byte,
) error {
	implementation, err := OpenProxyImplementation(proxy.GetProxyImplementation())
	if err != nil {
		return err
	}
	source, err := serviceproxy.PrepareRuntimeSource(implementation, sourceProxyConfig)
	if err != nil {
		return err
	}
	current, err := serviceproxy.PrepareRuntimeSource(implementation, proxy.GetProxyConfigJson())
	if err != nil {
		return err
	}
	configs := candidateRuntimeMappingValue(service, "configs")
	definitions := candidateRuntimeMappingValue(root, "configs")
	if configs == nil || configs.Kind != yaml.SequenceNode || definitions == nil ||
		definitions.Kind != yaml.MappingNode {
		return invalidCandidateRuntimeProxyConfig()
	}
	matches := 0
	var content *yaml.Node
	for _, entry := range configs.Content {
		name, include, err := candidateRuntimeResourceReference("configs", entry)
		if err != nil || !include {
			return invalidCandidateRuntimeProxyConfig()
		}
		definition := candidateRuntimeMappingValue(definitions, name)
		candidate := candidateRuntimeMappingValue(definition, "content")
		if candidate == nil || candidate.Kind != yaml.ScalarNode || candidate.Value != string(source.Content) {
			continue
		}
		matches++
		content = candidate
	}
	if matches != 1 || content == nil {
		return invalidCandidateRuntimeProxyConfig()
	}
	content.Tag = "!!str"
	content.Value = string(current.Content)
	return nil
}

func invalidCandidateRuntimeProxyConfig() error {
	return errs.New(errs.KindValidationFailed, "candidate runtime proxy configuration is invalid")
}

func candidateRuntimePruneResources(root *yaml.Node, section string, referenced map[string]bool) {
	index := candidateRuntimeMappingIndex(root, section)
	if index < 0 {
		return
	}
	resources := root.Content[index+1]
	if resources.Kind != yaml.MappingNode {
		return
	}
	kept := resources.Content[:0]
	for offset := 0; offset+1 < len(resources.Content); offset += 2 {
		if referenced[resources.Content[offset].Value] {
			kept = append(kept, resources.Content[offset], resources.Content[offset+1])
		}
	}
	resources.Content = kept
	if len(kept) == 0 {
		root.Content = append(root.Content[:index], root.Content[index+2:]...)
	}
}

func candidateRuntimeMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	index := candidateRuntimeMappingIndex(mapping, key)
	if index < 0 {
		return nil
	}
	return mapping.Content[index+1]
}

func candidateRuntimeMappingIndex(mapping *yaml.Node, key string) int {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return -1
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Kind == yaml.ScalarNode && mapping.Content[index].Value == key {
			return index
		}
	}
	return -1
}
