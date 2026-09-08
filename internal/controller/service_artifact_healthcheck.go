package controller

import (
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	"gopkg.in/yaml.v3"
)

func serviceNodeHasHealthcheck(service *yaml.Node) bool {
	index := mappingIndex(service, "healthcheck")
	if index < 0 || service.Content[index+1].Kind != yaml.MappingNode {
		return false
	}
	health := service.Content[index+1]
	if disabled := mappingIndex(health, "disable"); disabled >= 0 {
		var value bool
		if health.Content[disabled+1].Decode(&value) != nil || value {
			return false
		}
	}
	if test := mappingIndex(health, "test"); test >= 0 {
		value := health.Content[test+1]
		return value.Kind != yaml.SequenceNode || len(value.Content) == 0 || value.Content[0].Value != "NONE"
	}
	return true
}

// Only the exact typed rendering is editable through the typed healthcheck
// control. Extra native fields and arbitrary commands retain Blueprint authority.
func typedServiceHealthcheck(node *yaml.Node) bool {
	var native struct {
		Test        []string `yaml:"test"`
		Interval    string   `yaml:"interval"`
		Timeout     string   `yaml:"timeout"`
		StartPeriod string   `yaml:"start_period"`
		Retries     int      `yaml:"retries"`
	}
	if node.Kind != yaml.MappingNode || node.Decode(&native) != nil ||
		len(native.Test) != 2 || native.Test[0] != "CMD-SHELL" {
		return false
	}
	health := core.Healthcheck{
		Interval: native.Interval, Timeout: native.Timeout, StartPeriod: native.StartPeriod, Retries: native.Retries,
	}
	command := native.Test[1]
	switch {
	case strings.HasPrefix(command, "curl -fsS -- ") && strings.HasSuffix(command, " >/dev/null"):
		url := unquoteHealthcheckValue(strings.TrimSuffix(strings.TrimPrefix(command, "curl -fsS -- "), " >/dev/null"))
		health.HTTP = strings.TrimPrefix(url, "http://127.0.0.1")
	case strings.HasPrefix(command, "nc -z -- "):
		host, port, ok := strings.Cut(strings.TrimPrefix(command, "nc -z -- "), "' '")
		if !ok {
			return false
		}
		health.TCP = unquoteHealthcheckValue(host+"'") + ":" + unquoteHealthcheckValue("'"+port)
	case strings.HasPrefix(command, "pgrep -f -- ") && strings.HasSuffix(command, " >/dev/null"):
		health.Pgrep = unquoteHealthcheckValue(
			strings.TrimSuffix(strings.TrimPrefix(command, "pgrep -f -- "), " >/dev/null"),
		)
	default:
		return false
	}
	return equalHealthcheckNodes(node, serviceHealthcheckNode(health))
}

func unquoteHealthcheckValue(value string) string {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return ""
	}
	return strings.ReplaceAll(value[1:len(value)-1], "'\"'\"'", "'")
}

func equalHealthcheckNodes(left, right *yaml.Node) bool {
	if left.Kind != right.Kind || left.Tag != right.Tag || left.Value != right.Value ||
		len(left.Content) != len(right.Content) {
		return false
	}
	if left.Kind == yaml.MappingNode {
		for index := 0; index < len(left.Content); index += 2 {
			other := mappingIndex(right, left.Content[index].Value)
			if other < 0 || !equalHealthcheckNodes(left.Content[index+1], right.Content[other+1]) {
				return false
			}
		}
		return true
	}
	for index := range left.Content {
		if !equalHealthcheckNodes(left.Content[index], right.Content[index]) {
			return false
		}
	}
	return true
}

func serviceHealthcheckNode(health core.Healthcheck) *yaml.Node {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	command := ""
	switch {
	case health.HTTP != "":
		command = "curl -fsS -- " + shellQuote("http://127.0.0.1"+health.HTTP) + " >/dev/null"
	case health.TCP != "":
		host, port, _ := strings.Cut(health.TCP, ":")
		command = "nc -z -- " + shellQuote(host) + " " + shellQuote(port)
	default:
		command = "pgrep -f -- " + shellQuote(health.Pgrep) + " >/dev/null"
	}
	test := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	test.Content = append(test.Content, scalarNode("CMD-SHELL"), scalarNode(command))
	appendMappingValue(node, "test", test)
	for _, field := range []struct{ key, value string }{
		{"interval", health.Interval}, {"timeout", health.Timeout}, {"start_period", health.StartPeriod},
	} {
		if field.value != "" {
			appendMappingValue(node, field.key, scalarNode(field.value))
		}
	}
	if health.Retries != 0 {
		appendMappingValue(
			node,
			"retries",
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(health.Retries)},
		)
	}
	return node
}
