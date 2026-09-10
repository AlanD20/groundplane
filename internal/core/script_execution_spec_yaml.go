package core

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// Node fields retain presence and scalar types while yaml.v3 resolves merge
// precedence. An inline typed remainder closes unknown fields at this boundary.
type scriptExecutionYAML struct {
	Mode    yaml.Node            `yaml:"mode"`
	Image   yaml.Node            `yaml:"image"`
	User    yaml.Node            `yaml:"user"`
	Volumes yaml.Node            `yaml:"volumes"`
	Entries yaml.Node            `yaml:"entries"`
	Unknown map[string]yaml.Node `yaml:",inline"`
}

// UnmarshalYAML is shared by Blueprint and CLI authoring. Null/string coercion
// cannot supply a decision; aliases and merges cannot erase field presence.
func (spec *ScriptExecutionSpec) UnmarshalYAML(node *yaml.Node) error {
	var fields scriptExecutionYAML
	if node.Kind != yaml.MappingNode || node.Decode(&fields) != nil || len(fields.Unknown) != 0 ||
		!scriptYAMLScalar(&fields.Mode, "!!str") {
		return invalidScriptExecutionYAML()
	}
	mode := scriptYAMLNode(&fields.Mode).Value
	if mode == string(ScriptExecutionInherited) &&
		(fields.Image.Kind != 0 || fields.User.Kind != 0 || fields.Volumes.Kind != 0 || fields.Entries.Kind != 0) {
		return invalidScriptExecutionYAML()
	}
	if mode == string(ScriptExecutionExplicit) &&
		(!scriptYAMLScalar(&fields.Image, "!!str") || !scriptYAMLScalar(&fields.User, "!!str")) {
		return invalidScriptExecutionYAML()
	}
	if fields.Volumes.Kind != 0 {
		volumes := scriptYAMLNode(&fields.Volumes)
		if volumes.Kind != yaml.SequenceNode {
			return invalidScriptExecutionYAML()
		}
		for _, grant := range volumes.Content {
			if err := validateScriptVolumeGrantYAML(scriptYAMLNode(grant)); err != nil {
				return err
			}
		}
	}
	if fields.Entries.Kind != 0 {
		entries := scriptYAMLNode(&fields.Entries)
		if entries.Kind != yaml.SequenceNode {
			return invalidScriptExecutionYAML()
		}
		for _, entry := range entries.Content {
			if !scriptYAMLScalar(entry, "!!str") {
				return invalidScriptExecutionYAML()
			}
		}
	}
	type plainScriptExecutionSpec ScriptExecutionSpec
	var decoded plainScriptExecutionSpec
	if node.Decode(&decoded) != nil {
		return invalidScriptExecutionYAML()
	}
	if err := ScriptExecutionSpec(decoded).Validate(); err != nil {
		return err
	}
	*spec = ScriptExecutionSpec(decoded)
	return nil
}

func validateScriptVolumeGrantYAML(node *yaml.Node) error {
	var fields struct {
		Volume   yaml.Node            `yaml:"volume"`
		Target   yaml.Node            `yaml:"target"`
		ReadOnly yaml.Node            `yaml:"read_only"`
		Unknown  map[string]yaml.Node `yaml:",inline"`
	}
	if node.Kind != yaml.MappingNode || node.Decode(&fields) != nil || len(fields.Unknown) != 0 ||
		!scriptYAMLScalar(&fields.Volume, "!!str") || !scriptYAMLScalar(&fields.Target, "!!str") ||
		!scriptYAMLScalar(&fields.ReadOnly, "!!bool") {
		return invalidScriptExecutionYAML()
	}
	return nil
}

func scriptYAMLScalar(node *yaml.Node, tag string) bool {
	resolved := scriptYAMLNode(node)
	return resolved.Kind == yaml.ScalarNode && resolved.Tag == tag
}

func scriptYAMLNode(node *yaml.Node) *yaml.Node {
	for depth := 0; node != nil && depth <= 16; depth++ {
		if node.Kind != yaml.AliasNode {
			return node
		}
		node = node.Alias
	}
	return &yaml.Node{}
}

func invalidScriptExecutionYAML() error {
	return errs.New(
		errs.KindValidationFailed,
		"script execution requires a closed context with explicit typed decisions",
	)
}
