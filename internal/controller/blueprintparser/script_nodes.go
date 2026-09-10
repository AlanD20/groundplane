package blueprintparser

import "gopkg.in/yaml.v3"

// YAML's decoders coerce floats and null. Retain node presence while the YAML
// parser resolves aliases/merge precedence; the complete schema is decoded next.
func validateScriptNodes(root *yaml.Node) error {
	for index := 0; index+1 < len(root.Content); index += 2 {
		if root.Content[index].Value != "x-gp-scripts" {
			continue
		}
		var scripts map[string]struct {
			Order     yaml.Node            `yaml:"order"`
			Execution yaml.Node            `yaml:"execution"`
			Other     map[string]yaml.Node `yaml:",inline"`
		}
		if root.Content[index+1].Decode(&scripts) != nil {
			return validationError("x-gp-scripts must be a Script mapping")
		}
		for _, script := range scripts {
			order := resolvedScriptNode(&script.Order)
			if order.Kind != 0 && (order.Kind != yaml.ScalarNode || order.Tag != "!!int") {
				return validationError("x-gp-scripts order must be an integer from 0 through 65535")
			}
			execution := resolvedScriptNode(&script.Execution)
			if execution.Kind != 0 && execution.Kind != yaml.MappingNode {
				return validationError("x-gp-scripts execution must be a context mapping")
			}
		}
	}
	return nil
}

func resolvedScriptNode(node *yaml.Node) *yaml.Node {
	// Preflight has already proved the alias graph is acyclic and depth-bounded.
	for node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	return node
}
