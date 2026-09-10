package blueprintparser

import "gopkg.in/yaml.v3"

// YAML's uint16 decoder coerces floats and null; preserve the authored integer
// contract before decoding the complete strict-key Script schema.
func validateScriptOrderNodes(root *yaml.Node) error {
	for index := 0; index+1 < len(root.Content); index += 2 {
		if root.Content[index].Value != "x-gp-scripts" {
			continue
		}
		scripts := root.Content[index+1]
		if scripts.Kind != yaml.MappingNode {
			continue // The strict schema decoder reports the container shape.
		}
		for scriptIndex := 1; scriptIndex < len(scripts.Content); scriptIndex += 2 {
			script := scripts.Content[scriptIndex]
			if script.Kind != yaml.MappingNode {
				continue
			}
			for field := 0; field+1 < len(script.Content); field += 2 {
				if script.Content[field].Value == "order" {
					value := script.Content[field+1]
					if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
						return validationError("x-gp-scripts order must be an integer from 0 through 65535")
					}
				}
			}
		}
	}
	return nil
}
