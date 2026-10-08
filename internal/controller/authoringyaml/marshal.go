// Package authoringyaml formats operator-facing YAML projections.
package authoringyaml

import (
	"bytes"

	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// Marshal expands collections in an owned authoring tree and uses two-space
// indentation. Scalar styles and values are preserved. Callers must not pass a
// shared canonical deployment tree: this formatting is for read projections.
func Marshal(document *yaml.Node) ([]byte, error) {
	blockCollections(document)
	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(document); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := encoder.Close(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return output.Bytes(), nil
}

func blockCollections(node *yaml.Node) {
	if node.Kind == yaml.MappingNode || node.Kind == yaml.SequenceNode {
		node.Style &^= yaml.FlowStyle
	}
	for _, child := range node.Content {
		blockCollections(child)
	}
}
