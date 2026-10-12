package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"gopkg.in/yaml.v3"
)

// Shared workloads must agree on Docker resource identity. The Zone's DNS
// decision is Controller policy: it selects listener access and workload DNS,
// not a different Docker bridge. Keep every physical field and ownership check.
func sharedRuntimeResourceEqual(section string, left, right *yaml.Node) bool {
	if section != "networks" {
		return attachRuntimeYAMLEqual(left, right)
	}
	before, validBefore := physicalRuntimeNetwork(left)
	after, validAfter := physicalRuntimeNetwork(right)
	return validBefore && validAfter && attachRuntimeYAMLEqual(before, after)
}

func physicalRuntimeNetwork(network *yaml.Node) (*yaml.Node, bool) {
	if network == nil || network.Kind != yaml.MappingNode || len(network.Content)%2 != 0 {
		return nil, false
	}
	physical := *network
	physical.Content = make([]*yaml.Node, 0, len(network.Content))
	seenDNS := false
	for index := 0; index < len(network.Content); index += 2 {
		key, value := network.Content[index], network.Content[index+1]
		if key.Value == zonedns.Extension {
			if seenDNS || key.Kind != yaml.ScalarNode || key.Tag != "!!str" ||
				value.Kind != yaml.ScalarNode || value.Tag != "!!bool" ||
				(value.Value != "true" && value.Value != "false") {
				return nil, false
			}
			seenDNS = true
			continue
		}
		physical.Content = append(physical.Content, key, value)
	}
	return &physical, true
}
