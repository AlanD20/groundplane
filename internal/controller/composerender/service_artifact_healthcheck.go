package composerender

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/core"
	"gopkg.in/yaml.v3"
)

func serviceNodeHasHealthcheck(service *yaml.Node) bool {
	index := MappingIndex(service, "healthcheck")
	if index < 0 {
		return false
	}
	var health core.Healthcheck
	if service.Content[index+1].Decode(&health) != nil {
		return false
	}
	return health.Enabled()
}

func serviceHealthcheckNode(health core.Healthcheck) *yaml.Node {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	if len(health.Test) > 0 {
		test := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		for _, arg := range health.Test {
			test.Content = append(test.Content, scalarNode(arg))
		}
		AppendMappingValue(node, "test", test)
	}
	if health.Disable {
		AppendMappingValue(node, "disable", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
	}
	for _, field := range []struct{ key, value string }{
		{"interval", health.Interval}, {"timeout", health.Timeout},
		{"start_period", health.StartPeriod}, {"start_interval", health.StartInterval},
	} {
		if field.value != "" {
			AppendMappingValue(node, field.key, scalarNode(field.value))
		}
	}
	if health.Retries != 0 {
		AppendMappingValue(
			node,
			"retries",
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(health.Retries)},
		)
	}
	return node
}
