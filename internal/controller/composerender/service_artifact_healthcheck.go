package composerender

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/compose-spec/compose-go/v2/types"
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

// ProjectServiceHealthcheck retains every native check field in the editable
// desired model; Blueprint imports and direct edits use the same representation.
func ProjectServiceHealthcheck(value *types.HealthCheckConfig) core.Healthcheck {
	if value == nil {
		return core.Healthcheck{}
	}
	result := core.Healthcheck{Test: append([]string(nil), value.Test...), Disable: value.Disable}
	for _, field := range []struct {
		source *types.Duration
		target *string
	}{
		{value.Interval, &result.Interval}, {value.Timeout, &result.Timeout},
		{value.StartPeriod, &result.StartPeriod}, {value.StartInterval, &result.StartInterval},
	} {
		if field.source != nil {
			*field.target = field.source.String()
		}
	}
	if value.Retries != nil {
		result.Retries = int(*value.Retries)
	}
	return result
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
